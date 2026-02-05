# pg_vaccumen — vacuum acumen for Aurora PostgreSQL

Prevents emergency autovacuum spikes by proactively vacuuming tables before they hit `autovacuum_freeze_max_age`. Spreads vacuum work over nightly runs to create a steady plateau instead of spikes.

## Problem

High-transaction Aurora PostgreSQL databases constantly bump against `autovacuum_freeze_max_age` (200M default), triggering emergency autovacuum that causes performance spikes. At 112M transactions/day, you burn through the 200M limit in under 2 days.

## Solution

Run this script nightly to vacuum tables before they reach the threshold:

- **Proactive**: Vacuums at 50% of threshold (configurable), not waiting for emergency
- **Adaptive**: Reads `autovacuum_freeze_max_age` from DB, adjusts when parameter changes
- **Blocker-aware**: Detects long transactions, replication slots, and prepared transactions that prevent vacuum progress
- **Jenkins-ready**: Exit codes for CI/CD integration

## Locking Behavior

**This script is safe to run during production hours.** Regular `VACUUM` (not `VACUUM FULL`) does not block normal database operations.

### What lock does VACUUM take?

VACUUM acquires a `ShareUpdateExclusiveLock` on each table, which:

| Operation | Blocked? |
|-----------|----------|
| SELECT | No |
| INSERT | No |
| UPDATE | No |
| DELETE | No |
| Other VACUUM on same table | Yes |
| DDL (ALTER TABLE, DROP, TRUNCATE) | Yes |
| CREATE INDEX CONCURRENTLY | Yes |

### What this means in practice

- **Reads and writes continue normally** while vacuum runs
- Vacuum may take longer if there's heavy write activity (it yields to writers)
- DDL operations will wait for vacuum to finish on that table
- Running multiple vacuums on the same table will serialize

### VACUUM FULL is different

`VACUUM FULL` rewrites the entire table and **does block all operations**. This script uses regular `VACUUM ANALYZE VERBOSE`, which does not block.

### VACUUM FREEZE - what we're trying to avoid

When a table's `relfrozenxid` age hits `autovacuum_freeze_max_age`, PostgreSQL triggers an **aggressive anti-wraparound vacuum** that is essentially a forced `VACUUM FREEZE`:

| Vacuum Type | Freezes Rows | Behavior |
|-------------|--------------|----------|
| Regular VACUUM | Only rows older than `vacuum_freeze_min_age` (50M default) | Opportunistic, spreads work over time |
| VACUUM FREEZE | All rows, regardless of age | Aggressive, must scan entire table |
| Emergency autovacuum | All rows (like FREEZE) | Cannot be canceled, high priority, causes spikes |

**Why emergency freeze vacuum is expensive:**

1. **Full table scan** - must examine every page, not just recently-modified ones
2. **More I/O** - rewrites pages to mark rows as frozen
3. **Cannot be deferred** - runs at high priority, won't yield to other operations
4. **Unpredictable timing** - kicks in when threshold hit, often during peak hours

**Our proactive approach:**

This script runs regular `VACUUM ANALYZE` which freezes rows incrementally (those older than `vacuum_freeze_min_age`). By vacuuming tables before they hit `autovacuum_freeze_max_age`, we:

- Spread freeze work across many nightly runs
- Keep each vacuum operation smaller and faster
- Maintain predictable, scheduled maintenance windows
- Avoid emergency anti-wraparound vacuum entirely

### Autovacuum vs manual vacuum

The locking behavior is identical whether vacuum is triggered by autovacuum or run manually by this script. The difference is timing and control—this script lets you spread the work predictably rather than having emergency autovacuum kick in during peak hours.

### What if autovacuum is already running?

| Scenario | What happens |
|----------|--------------|
| Autovacuum is vacuuming table X, script tries to vacuum X | Script waits for autovacuum to finish, then vacuums |
| Script is vacuuming table X, autovacuum wants table X | Autovacuum skips that table (yields to manual vacuum) |
| Both want different tables | Both run concurrently, no conflict |

PostgreSQL's autovacuum is designed to be polite—it yields to manual operations. If this script is vacuuming a table, autovacuum won't fight for it. Conversely, if autovacuum got there first, your manual vacuum simply waits its turn.

**Bottom line**: Running this script won't cause deadlocks or conflicts with autovacuum. At worst, you wait briefly for an in-progress autovacuum to finish on a specific table.

## Blockers: What Prevents Vacuum Progress

VACUUM can run, but it can't advance `relfrozenxid` past certain obstacles. These "blockers" hold back the freeze horizon, meaning your vacuum work doesn't actually buy you headroom.

### Replication Slots

**Why they matter**: Logical replication slots maintain an `xmin` value representing the oldest transaction the subscriber might still need. PostgreSQL cannot vacuum away any rows that might be needed by that slot.

| Slot State | Impact |
|------------|--------|
| Active, caught up | No impact - xmin advances with replication |
| Active, lagging | xmin falls behind, blocks vacuum progress |
| Inactive (orphaned) | xmin frozen at creation time, severe blocker |

**How to check**:

```sql
-- Find slots with old xmin (potential blockers)
select slot_name, slot_type, active,
       age(xmin) as xmin_age,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
from pg_replication_slots
where xmin is not null
order by age(xmin) desc;
```

**What to do**:

| Situation | Action |
|-----------|--------|
| Lagging active slot | Investigate subscriber - is it running? Network issues? |
| Orphaned inactive slot | Drop it: `select pg_drop_replication_slot('slot_name');` |
| Consistently slow subscriber | Consider increasing subscriber resources or filtering replicated tables |

**Risk of orphaned slots**: An inactive slot with a stale xmin can single-handedly prevent all vacuum progress across the entire database. If left unchecked, this leads to transaction ID wraparound regardless of how much you vacuum.

### Long-Running Transactions

**Why they matter**: Any open transaction holds a snapshot that prevents vacuum from removing rows deleted after the transaction started.

**How to check**:

```sql
-- Find transactions running longer than 1 hour
select pid, usename, state,
       age(backend_xid) as xid_age,
       now() - xact_start as duration,
       left(query, 60) as query
from pg_stat_activity
where xact_start < now() - interval '1 hour'
  and state != 'idle'
order by xact_start;
```

**What to do**:

| Situation | Action |
|-----------|--------|
| Idle in transaction | Application bug - should commit/rollback promptly |
| Long-running report | Schedule during maintenance windows, or use a replica |
| Stuck migration | Investigate and resolve, or terminate if safe |

### Prepared Transactions

**Why they matter**: Two-phase commit (`PREPARE TRANSACTION`) creates a transaction that persists even across server restarts, holding its snapshot indefinitely.

**How to check**:

```sql
select gid, prepared, owner, database,
       age(transaction) as xid_age
from pg_prepared_xacts
order by prepared;
```

**What to do**: Prepared transactions should be short-lived. If you find old ones:

```sql
-- Commit or rollback the prepared transaction
COMMIT PREPARED 'transaction_gid';
-- or
ROLLBACK PREPARED 'transaction_gid';
```

### The --force Flag

This script exits with an error if blockers are detected, because vacuuming with blockers present won't help. Use `--force` only when you understand the blocker and have a plan to resolve it—forcing vacuum to run just wastes I/O.

## Combined Strategy: Higher Threshold + Proactive Maintenance

The recommended approach is to **both** increase `autovacuum_freeze_max_age` **and** run this nightly maintenance. Here's why:

### Increasing `autovacuum_freeze_max_age` alone

| Setting | Headroom at 112M txns/day | Problem |
|---------|---------------------------|---------|
| 200M (default) | ~1.8 days | Emergency vacuum every 2 days |
| 750M (Aurora max) | ~6.7 days | Buys time, but still hits emergency eventually |

Raising the threshold alone just delays the problem. You still get emergency anti-wraparound vacuum, just less frequently. Each emergency vacuum is actually **worse** because tables have accumulated more to freeze.

### Proactive maintenance alone (at 200M threshold)

With only ~2 days of headroom, you're racing against the clock:
- Miss one nightly run? Emergency vacuum.
- Jenkins agent down? Emergency vacuum.
- Holiday weekend? Emergency vacuum.

### Combined approach (750M + nightly maintenance)

| Metric | Value |
|--------|-------|
| Emergency threshold | 750M |
| Proactive vacuum at | 375M (50% of max) |
| Headroom after proactive vacuum | ~375M |
| Days of buffer at 112M txns/day | ~3.3 days |
| Warning alert at | 600M (80%) |
| Critical alert at | 712M (95%) |

**Benefits:**

1. **Resilience** - Miss a night or two? Still have days of buffer before emergency
2. **Smaller vacuums** - Tables vacuumed at 375M vs 750M = less work per vacuum
3. **Predictable scheduling** - Vacuum during maintenance windows, not during peak traffic
4. **Early warning** - Alerts at 80%/95% give time to react before emergency
5. **No emergency vacuums** - With proper tuning, emergency anti-wraparound never triggers

### Threshold tuning strategies

The `--threshold` option controls when tables are selected for vacuum. Lower threshold = more frequent, smaller vacuums. Here's a comparison at 112M transactions/day:

| Strategy | `autovacuum_freeze_max_age` | `--threshold` | Runway to emergency | Vacuums per table |
|----------|----------------------------|---------------|---------------------|-------------------|
| **Current (no action)** | 200M | N/A | **~11 hours** | Emergency only |
| Conservative | 750M | 250M (33%) | ~4.5 days | Every ~2.2 days |
| Balanced | 750M | 375M (50%) | ~3.3 days | Every ~3.3 days |
| Relaxed | 750M | 500M (67%) | ~2.2 days | Every ~4.5 days |

**Recommendation by workload:**

| Scenario | Suggested threshold | Why |
|----------|---------------------|-----|
| High-transaction, critical system | 250M (33%) | Maximum safety buffer, smallest vacuum operations |
| Moderate transaction volume | 375M (50%) | Good balance of safety and efficiency |
| Low-transaction or dev/staging | 500M+ (67%+) | Less frequent maintenance needed |

**Note on `--limit`:** Lower thresholds select more tables per run. If using 250M threshold, you may need `--limit 50` or higher to keep up with the backlog.

### Example timeline

```
Day 1: Tables at 375M (50%), nightly vacuum runs, resets to ~50M
Day 2: Tables at ~162M, nightly vacuum runs (nothing to do, all under 50%)
Day 3: Tables at ~274M, nightly vacuum runs (nothing to do)
Day 4: Tables at ~386M, nightly vacuum runs, resets highest tables
...
```

The maintenance becomes routine and boring—exactly what you want.

## Installation

```bash
pip install -r requirements.txt
```

Requirements:
- Python 3.10+
- boto3 (AWS SDK)
- psycopg (PostgreSQL adapter, v3)

## Quick Start

```bash
# Dry-run (default) - shows what would be vacuumed
python pg_vaccumen.py --host mydb.cluster-xxx.us-east-1.rds.amazonaws.com \
    --database mydb --db-username postgres --db-password secret

# Execute vacuum
python pg_vaccumen.py --host mydb.cluster-xxx.us-east-1.rds.amazonaws.com \
    --database mydb --db-username postgres --db-password secret --execute

# Using AWS cluster lookup (requires IAM permissions)
python pg_vaccumen.py --cluster my-aurora-cluster --database mydb
```

## Command-Line Options

### Connection Options

| Option | Description |
|--------|-------------|
| `--cluster` | Aurora cluster identifier (uses boto3 to lookup endpoint) |
| `--host` | Database host (bypasses AWS lookup) |
| `--database` | Database name (required) |
| `--region` | AWS region (default: us-east-1) |
| `--db-username` | Database username (default: postgres) |
| `--db-password` | Database password (required with --host, otherwise uses Secrets Manager) |

### Threshold Options

| Option | Default | Description |
|--------|---------|-------------|
| `--threshold` | 50% of max | Select tables with relfrozenxid age above this value |
| `--warning-pct` | 80 | Alert warning when oldest table exceeds this % of max |
| `--critical-pct` | 95 | Alert critical when oldest table exceeds this % of max |
| `--limit` | 10 | Maximum tables to vacuum per run |

### Execution Options

| Option | Description |
|--------|-------------|
| `--execute` | Actually run vacuum (default is dry-run) |
| `--force` | Proceed despite blockers (long txns, replication slots) |
| `--skip-preflight` | Skip preflight checks (not recommended) |
| `--statement-timeout` | Timeout in seconds for vacuum operations (default: 0 = no timeout) |

### Transaction Rate Tracking

| Option | Description |
|--------|-------------|
| `--baseline-to-db` | Store/load baseline from `vacuum_baseline` table (recommended) |
| `--baseline-file` | JSON file to store/load transaction baseline (local/testing) |
| `--save-baseline` | Save current stats as new baseline (use with either option above) |

### Metrics and Instrumentation

| Option | Description |
|--------|-------------|
| `--metrics-file` | JSON file to append vacuum metrics (duration, size, etc.) |
| `--metrics-to-db` | Store metrics in `vacuum_metrics` table (recommended for Jenkins) |

## Exit Codes

| Code | Meaning | Jenkins Result |
|------|---------|----------------|
| 0 | OK - maintenance keeping up | SUCCESS |
| 1 | Error - connection failure or blockers | FAILURE |
| 2 | Warning - tables approaching threshold | UNSTABLE |
| 3 | Critical - tables at/past threshold | FAILURE |

## Transaction Rate Tracking

The script estimates "days until autovacuum" based on transaction rate. Since Aurora often has `stats_reset` as NULL in `pg_stat_database`, you can track your own baseline.

### Why manual baseline is better than pg_stat_reset

| Factor | `pg_stat_reset()` (native) | Manual baseline (`--baseline-to-db`) |
|--------|---------------------------|--------------------------------------|
| Survives Aurora failover | No - resets on restart | Yes - persists in table |
| Survives `pg_stat_reset()` calls | No | Yes |
| Survives major version upgrade | Maybe - depends on method | Yes |
| Control over measurement window | Limited - resets everything | Full - reset when you want |
| Extra maintenance | None | One table |
| Accuracy | Excellent | Excellent |

**Recommendation:** Use `--baseline-to-db` as your primary method. It provides:

1. **Failover resilience** - Aurora failovers reset `pg_stat_database` stats, but your baseline table survives
2. **Upgrade resilience** - Major version upgrades (blue/green, snapshot restore) may reset native stats
3. **Independence** - Won't be affected if someone runs `pg_stat_reset()` for other reasons
4. **Works when stats_reset is NULL** - Many Aurora instances have never had stats reset

After a major version upgrade, you could *also* run `SELECT pg_stat_reset();` to have both options available as a fallback.

### Option 1: Store baseline in PostgreSQL (recommended for Jenkins)

```bash
# First run: save baseline
python pg_vaccumen.py --host ... --baseline-to-db --save-baseline

# Subsequent runs: calculate rate from baseline
python pg_vaccumen.py --host ... --baseline-to-db
```

This creates `vacuum_baseline` table with one row per database.

### Option 2: Store baseline in JSON file (local/testing)

```bash
# First run: save baseline
python pg_vaccumen.py --host ... --baseline-file /var/lib/vacuum_baseline.json --save-baseline

# Subsequent runs: calculate rate from baseline
python pg_vaccumen.py --host ... --baseline-file /var/lib/vacuum_baseline.json
```

The baseline stores timestamp and transaction count. On subsequent runs, the script calculates the delta to determine transactions/day.

**Tip**: After a major version upgrade, run `SELECT pg_stat_reset();` to establish a native baseline in `pg_stat_database`, or use `--save-baseline` to reset your tracked baseline.

## Vacuum Metrics

### Option 1: Store in PostgreSQL (recommended for Jenkins)

Use `--metrics-to-db` to store metrics in the database itself. The table is created automatically:

```bash
python pg_vaccumen.py --host ... --execute --metrics-to-db
```

This creates `vacuum_metrics`:

```sql
create table vacuum_metrics (
    id serial primary key,
    vacuumed_at timestamptz default now(),
    table_name text not null,
    age_before bigint,
    size_bytes bigint,
    duration_seconds numeric(10,2),
    database_name text,
    cluster_host text
);
```

**Query examples:**

```sql
-- Slowest tables on average
select table_name,
       count(*) as vacuum_count,
       round(avg(duration_seconds), 1) as avg_seconds,
       round(avg(size_bytes)/1e9, 2) as avg_gb
from vacuum_metrics
group by table_name
order by avg(duration_seconds) desc
limit 20;

-- Duration trend over time
select date_trunc('day', vacuumed_at) as day,
       count(*) as tables_vacuumed,
       round(sum(duration_seconds), 0) as total_seconds
from vacuum_metrics
group by 1 order by 1;

-- Tables vacuumed most frequently (hottest)
select table_name, count(*) as times_vacuumed
from vacuum_metrics
where vacuumed_at > now() - interval '30 days'
group by table_name
order by count(*) desc
limit 20;

-- Vacuum efficiency (MB per second) by table
select table_name,
       count(*) as vacuum_count,
       round(avg(size_bytes)/1e6, 1) as avg_mb,
       round(avg(duration_seconds), 1) as avg_seconds,
       round(avg(size_bytes/1e6 / nullif(duration_seconds, 0)), 1) as mb_per_second
from vacuum_metrics
group by table_name
order by avg_mb desc
limit 20;

-- Weekly vacuum summary
select date_trunc('week', vacuumed_at) as week,
       count(*) as tables_vacuumed,
       count(distinct table_name) as unique_tables,
       round(sum(duration_seconds)/60, 1) as total_minutes,
       round(sum(size_bytes)/1e9, 2) as total_gb_processed
from vacuum_metrics
group by 1 order by 1;

-- Tables with increasing vacuum duration (potential degradation)
select table_name,
       round(avg(case when vacuumed_at < now() - interval '7 days'
                      then duration_seconds end), 1) as avg_duration_older,
       round(avg(case when vacuumed_at >= now() - interval '7 days'
                      then duration_seconds end), 1) as avg_duration_recent,
       count(*) as total_vacuums
from vacuum_metrics
group by table_name
having count(*) >= 4
   and avg(case when vacuumed_at >= now() - interval '7 days' then duration_seconds end) >
       avg(case when vacuumed_at < now() - interval '7 days' then duration_seconds end) * 1.5
order by avg_duration_recent desc;

-- Average days between vacuums per table
select table_name,
       count(*) as vacuum_count,
       round(extract(epoch from (max(vacuumed_at) - min(vacuumed_at))) /
             nullif(count(*) - 1, 0) / 86400, 1) as avg_days_between
from vacuum_metrics
group by table_name
having count(*) > 1
order by avg_days_between desc
limit 20;

-- Last vacuum time per table (find stale tables)
select table_name,
       max(vacuumed_at) as last_vacuumed,
       round(extract(epoch from (now() - max(vacuumed_at)))/86400, 1) as days_since_vacuum,
       count(*) as total_vacuums
from vacuum_metrics
group by table_name
order by last_vacuumed asc
limit 20;

-- Largest tables processed
select table_name,
       round(max(size_bytes)/1e9, 2) as max_gb,
       round(avg(size_bytes)/1e9, 2) as avg_gb,
       round(max(duration_seconds), 1) as max_seconds,
       count(*) as vacuum_count
from vacuum_metrics
group by table_name
order by max_gb desc
limit 20;
```

### Option 2: Store in JSON file

Use `--metrics-file` for local/testing scenarios:

```bash
python pg_vaccumen.py --host ... --execute --metrics-file /var/lib/vacuum_metrics.json
```

Each vacuum appends a JSON record with the same fields.

### Analysis ideas

| Query | Purpose |
|-------|---------|
| Tables with longest vacuum duration | Identify candidates for partitioning |
| Duration vs size correlation | Validate I/O performance |
| Duration trends over time | Detect degradation |
| Tables vacuumed most frequently | Hottest tables for per-table tuning |

## Sample Output

```
Autovacuum freeze max age: 200,000,000
Table selection threshold: 100,000,000 (50% of max)

Transaction Rate:
  Stats reset at:                (never)
  Total transactions recorded:   26,470,074,728
  Transactions per day:          112,221,507 (from baseline, 1.0 days)
  Est. time to autovacuum:       11.3 hours  *** URGENT ***

Alert Status: OK: Oldest XID age at 73% of autovacuum threshold

Tables to vacuum (10 of 244 total exceeding threshold):
--------------------------------------------------------------------------------
Table                                                     Age     % of Max
--------------------------------------------------------------------------------
assets                                            146,935,397          73%
asset_used_audit                       146,934,924          73%
...

RECOMMENDATIONS
  • URGENT: Only 11.3 hours until autovacuum triggers. Increase --limit significantly (try 40) or run more frequently.
  • Backlog: 234 additional table(s) waiting beyond --limit of 10. Total needing vacuum: 244. Consider --limit 30.
```

## Tuning Guidance

| Symptom | Action |
|---------|--------|
| "Days until autovacuum" shrinking | Increase `--limit` or run more frequently |
| Hitting `--limit` with large backlog | Increase `--limit` (try 2-3x) |
| >30 days headroom, no tables to vacuum | Reduce frequency or raise `--threshold` |
| Blockers detected | Investigate long-running transactions or lagging replication slots |

## Jenkins Integration

The included `Jenkinsfile` provides a parameterized pipeline for nightly runs.

### Pipeline Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CLUSTER` | | Aurora cluster identifier (or use HOST) |
| `HOST` | | Database host directly (or use CLUSTER) |
| `DATABASE` | my_db | Database name |
| `REGION` | us-east-1 | AWS region |
| `DB_USERNAME` | postgres | Database username |
| `DB_PASSWORD` | | Database password |
| `THRESHOLD` | | Table selection threshold (empty = 50% of max) |
| `WARNING_PCT` | 80 | Warning alert percentage |
| `CRITICAL_PCT` | 95 | Critical alert percentage |
| `LIMIT` | 10 | Max tables per run |
| `EXECUTE` | false | Actually vacuum (false = dry-run) |
| `FORCE` | false | Proceed despite blockers |
| `METRICS_TO_DB` | true | Store metrics in database |
| `BASELINE_TO_DB` | true | Store baseline in database |
| `SAVE_BASELINE` | false | Save new baseline this run |

### First Run Setup

On the first Jenkins run, establish the baseline:

```
EXECUTE=false        # Dry-run to verify everything works
SAVE_BASELINE=true   # Establish transaction rate baseline
METRICS_TO_DB=true   # Enable metrics collection
BASELINE_TO_DB=true  # Enable baseline tracking
```

This creates the tracking tables:
- `vacuum_baseline` - transaction rate baseline
- `vacuum_metrics` - vacuum performance history

### Subsequent Runs (Nightly)

```
EXECUTE=true         # Actually vacuum
SAVE_BASELINE=false  # Use existing baseline
METRICS_TO_DB=true   # Continue collecting metrics
BASELINE_TO_DB=true  # Use baseline for rate calculation
LIMIT=30             # Adjust based on backlog
```

### Re-establishing Baseline

After a major version upgrade or stats reset, re-establish the baseline:

```
SAVE_BASELINE=true   # Reset the baseline
```

### Key Features

- Dry-run by default (set EXECUTE=true to vacuum)
- Password handling via workspace file with cleanup
- Exit code mapping to Jenkins build status
- All data persists in database (no external storage needed)

## Database Tables

The script creates these tables automatically when using `--metrics-to-db` or `--baseline-to-db`:

### vacuum_metrics

Stores vacuum performance history for trend analysis:

```sql
create table vacuum_metrics (
    id serial primary key,
    vacuumed_at timestamptz default now(),
    table_name text not null,
    age_before bigint,
    size_bytes bigint,
    duration_seconds numeric(10,2),
    database_name text,
    cluster_host text
);
```

### vacuum_baseline

Stores transaction rate baseline (one row per database):

```sql
create table vacuum_baseline (
    id serial primary key,
    recorded_at timestamptz default now(),
    total_txns bigint not null,
    database_name text not null,
    cluster_host text,
    unique (database_name)
);
```

### Checking baseline status

```sql
-- View current baseline
select database_name, recorded_at, total_txns,
       round(extract(epoch from (now() - recorded_at))/3600, 1) as hours_old
from vacuum_baseline;

-- View recent vacuum history
select table_name, vacuumed_at, duration_seconds,
       round(size_bytes/1e6, 1) as size_mb
from vacuum_metrics
order by vacuumed_at desc
limit 20;
```

## Aurora PostgreSQL Notes

- **Max `autovacuum_freeze_max_age`**: 750M (vs 2B on open-source PostgreSQL)
- **Default `autovacuum_freeze_max_age`**: 200M
- **`vacuum_freeze_min_age`**: 50M (floor for row freezing)
- Aurora has safeguards and won't kill the database on wraparound like open-source PostgreSQL

## Files

| File | Description |
|------|-------------|
| `pg_vaccumen.py` | Main script |
| `Jenkinsfile` | Jenkins pipeline |
| `requirements.txt` | Python dependencies |

## Attribution

Inspired by [manual_vacuum.sh](https://github.com/omniti-labs/pgtreats/blob/master/tools/manual_vacuum.sh) from OmniTI's pgtreats.
