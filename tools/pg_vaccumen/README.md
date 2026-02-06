# pg_vaccumen — vacuum acumen for Aurora PostgreSQL

Prevents emergency autovacuum spikes by proactively vacuuming tables before they hit `autovacuum_freeze_max_age`. Spreads vacuum work over nightly runs to create a steady plateau instead of spikes.

## Problem

High-transaction Aurora PostgreSQL databases constantly bump against `autovacuum_freeze_max_age` (200M default), triggering emergency autovacuum that causes performance spikes. At 112M transactions/day, you burn through the 200M limit in under 2 days.

## Solution

Run this script nightly to vacuum tables before they reach the threshold:

- **Proactive**: Vacuums at 50% of threshold (configurable), not waiting for emergency
- **Adaptive**: Reads `autovacuum_freeze_max_age` from DB, adjusts when parameter changes
- **Blocker-aware**: Detects long transactions, replication slots, and prepared transactions that prevent vacuum progress
- **Autovacuum-aware**: Skips tables already being vacuumed by autovacuum, filters by size
- **Jenkins-ready**: Exit codes for CI/CD integration

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
python pg_vaccumen.py --host mydb.cluster-xxx.us-west-2.rds.amazonaws.com \
    --database mydb --db-username postgres --db-password secret

# Execute vacuum
python pg_vaccumen.py --host mydb.cluster-xxx.us-west-2.rds.amazonaws.com \
    --database mydb --db-username postgres --db-password secret --execute

# Using AWS cluster lookup (requires IAM permissions)
python pg_vaccumen.py --cluster my-aurora-cluster --database mydb

# Skip tables over 500 GB, vacuum up to 30 tables
python pg_vaccumen.py --host ... --database mydb --execute \
    --max-size 500 --limit 30 --metrics-to-db --baseline-to-db
```

## Sample Output

```
Autovacuum freeze max age: 200,000,000
Table selection threshold: 100,000,000 (50% of max)
Max tables per run: 30
Max table size: 500.0 GB
Skip autovacuum targets: yes

Alert Status: OK: Oldest XID age at 79% of autovacuum threshold

Tables to vacuum (30 of 139 total exceeding threshold):
  (5 table(s) excluded by --max-size 500.0 GB)
----------------------------------------------------------------------------------------------------
Table                                                Age   % of Max         Size               Note
----------------------------------------------------------------------------------------------------
status_key_values                             58,738,271        29%     118.9 GB
event_transaction_assignments                 57,922,905        28%      64.0 MB
state_cache                                   57,824,644        28%       1.0 GB [autovacuum running]
...
----------------------------------------------------------------------------------------------------
  ... and 109 more table(s) waiting

RECOMMENDATIONS
  * Backlog: 109 additional table(s) waiting beyond --limit of 30. Consider --limit 90.
```

## Real-World Scenario: Catching Up on a 135-Table Backlog

This walkthrough shows how to triage a production database where proactive vacuum has never run and most tables are well past the 50% threshold.

### Initial assessment

A production Aurora PostgreSQL database with `autovacuum_freeze_max_age` at 200M. First dry-run at default 50% threshold — also save the baseline for transaction rate tracking:

```bash
python pg_vaccumen.py --host ... --database mydb \
    --max-size 500 --force --check-bloat \
    --metrics-to-db --baseline-to-db --save-baseline
```

```
Alert Status: OK: Oldest XID age at 79% of autovacuum threshold (158,859,691 / 200,000,000)

Tables to vacuum (2):
  (2 table(s) excluded by --max-size 500.0 GB)
----------------------------------------------------------------------------------------------------
Table                                                Age   % of Max         Size               Note
----------------------------------------------------------------------------------------------------
event_log                                    158,859,694        79%    2680.1 GB [autovacuum running]
message_archive                              158,839,100        79%   16183.8 GB [autovacuum running]
----------------------------------------------------------------------------------------------------
```

Only 2 tables above 50%, both are massive (2.7 TB and 16 TB), and autovacuum is already running on both. With `--max-size 500` and auto-skip, both are filtered out — nothing to do at this threshold.

### Step 1: Lower the threshold to find the backlog

Drop to 25% (50M) to see what's waiting below the default threshold:

```bash
python pg_vaccumen.py --host ... --database mydb \
    --max-size 500 --force --limit 50 --threshold 50000000 \
    --check-bloat --metrics-to-db --baseline-to-db
```

```
Tables to vacuum (50 of 135 total exceeding threshold):
  (10 table(s) excluded by --max-size 500.0 GB)
----------------------------------------------------------------------------------------------------
Table                                                Age   % of Max         Size               Note
----------------------------------------------------------------------------------------------------
status_key_values                             58,738,271        29%     118.9 GB
event_transaction_assignments                 57,922,905        28%      64.0 MB
state_cache                                   57,824,644        28%       1.0 GB
enclosing_geofences                           56,873,134        28%       1.6 GB
sensor_statuses                               54,099,678        27%       5.0 MB
driver_status                                 51,896,819        25%      52.4 MB
events                                        50,986,818        25%      55.4 GB
raw_events                                    50,986,272        25%      50.0 GB
...
----------------------------------------------------------------------------------------------------
  ... and 85 more table(s) waiting

RECOMMENDATIONS
  * Backlog: 85 additional table(s) waiting beyond --limit of 50. Total needing vacuum: 135.
```

135 tables between 25-29% of the 200M threshold. Sizes range from 5 MB to 498 GB.

### Step 2: Vacuum the small tables first (parallel)

Start with smaller tables using parallel workers to make quick progress on the backlog. Wait at least 1 hour after the `--save-baseline` step so the transaction rate calculation has enough data:

```bash
# Vacuum tables under 100 GB, up to 50 per run, 2 workers
python pg_vaccumen.py --host ... --database mydb \
    --max-size 100 --force --limit 50 --threshold 50000000 \
    --workers 2 --execute \
    --check-bloat --metrics-to-db --baseline-to-db
```

This knocks out dozens of small tables quickly while skipping the 100+ GB tables that would take hours each. With `--workers 2`, two tables are vacuumed concurrently — the advisory lock system ensures no more than 2 total across all instances.

### Step 3: Increase size limit for bigger tables

Once the small tables are done, raise the size limit:

```bash
# Now vacuum tables up to 500 GB, fewer per run
python pg_vaccumen.py --host ... --database mydb \
    --max-size 500 --force --limit 10 --threshold 50000000 \
    --workers 2 --execute \
    --check-bloat --metrics-to-db --baseline-to-db
```

Fewer tables per run (`--limit 10`) since each one is larger and takes longer.

### Step 4: Return to normal nightly schedule

Once the backlog is cleared, **drop the `--threshold` flag** to return to the default 50% (100M):

```bash
# Nightly cron / Jenkins job
python pg_vaccumen.py --host ... --database mydb \
    --max-size 500 --limit 30 --workers 2 --execute \
    --force --check-bloat --metrics-to-db --baseline-to-db
```

> **Important:** Do not keep `--threshold 50000000` for nightly runs. The default `vacuum_freeze_min_age` is 50M, meaning VACUUM cannot freeze rows younger than 50M transactions — tables will never drop below ~50M age. A 25% threshold (50M at 200M max) causes tables to be re-vacuumed every run with no benefit. The default 50% threshold (100M) gives healthy headroom above the floor.

### Key takeaways

| Lesson | Detail |
|--------|--------|
| Start with dry-run + baseline | Save the baseline first, verify tables, then execute after 1+ hour |
| Lower threshold to find backlog | 25% threshold reveals tables hiding below the default 50% |
| Raise threshold once caught up | Keeping 25% causes infinite re-vacuum — tables can't drop below `vacuum_freeze_min_age` (50M) |
| Use `--max-size` to prioritize | Vacuum many small tables quickly before tackling large ones |
| Use `--workers 2` for backlogs | Advisory locks enforce the global limit — safe to run multiple instances |
| Auto-skip prevents wasted I/O | Don't re-vacuum what autovacuum or another instance is already handling |
| Increase `--limit` for backlogs | The recommendation engine tells you when to increase it |
| Work in phases | Small tables first, then medium, then large — each in separate runs if needed |

## Command-Line Options

### Connection Options

| Option | Description |
|--------|-------------|
| `--cluster` | Aurora cluster identifier (uses boto3 to lookup endpoint) |
| `--host` | Database host (bypasses AWS lookup) |
| `--database` | Database name (required) |
| `--region` | AWS region (default: us-west-2) |
| `--db-username` | Database username (default: postgres) |
| `--db-password` | Database password (required with --host, otherwise uses Secrets Manager) |

### Threshold Options

| Option | Default | Description |
|--------|---------|-------------|
| `--threshold` | 50% of max | Select tables with relfrozenxid age above this value |
| `--warning-pct` | 80 | Alert warning when oldest table exceeds this % of max |
| `--critical-pct` | 95 | Alert critical when oldest table exceeds this % of max |
| `--limit` | 10 | Maximum tables to vacuum per run |
| `--max-size` | No limit | Skip tables larger than this size in GB |
| `--no-skip-autovacuum` | Skip enabled | Don't skip tables currently being vacuumed by autovacuum |

### Bloat Analysis Options

| Option | Default | Description |
|--------|---------|-------------|
| `--check-bloat` | Off | Analyze tables for dead tuple bloat (high dead/live ratio) |
| `--bloat-pct` | 50.0 | Dead tuple percentage threshold for bloat analysis |

### Execution Options

| Option | Default | Description |
|--------|---------|-------------|
| `--execute` | Off | Actually run vacuum (default is dry-run) |
| `--workers` | 1 | Parallel vacuum workers — **global limit** across all instances via advisory locks. Capped at 8; auto-reduced if unsafe |
| `--force` | Off | Proceed despite blockers (long txns, replication slots) |
| `--skip-preflight` | Off | Skip preflight checks (not recommended) |
| `--statement-timeout` | 0 | Timeout in seconds for vacuum operations (0 = no timeout) |

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

## Tuning Guidance

| Symptom | Action |
|---------|--------|
| "Days until autovacuum" shrinking | Increase `--limit` or run more frequently |
| Hitting `--limit` with large backlog | Increase `--limit` (try 2-3x) |
| >30 days headroom, no tables to vacuum | Reduce frequency or raise `--threshold` |
| Blockers detected | Investigate long-running transactions or lagging replication slots |
| A few huge tables dominate the queue | Use `--max-size` to skip them, let autovacuum handle |
| Many tables skipped (autovacuum running) | Autovacuum is keeping up — focus `--limit` on remaining tables |
| Tables excluded by size need vacuuming | Run a separate job without `--max-size` during a longer window |
| High dead tuple ratio after recent vacuum | File-level bloat — use `pg_repack` or `VACUUM FULL` to reclaim space |
| `--check-bloat` shows many tables >50% | Autovacuum may be under-resourced — review `autovacuum_vacuum_cost_delay` and worker counts |
| Large backlog, need to catch up faster | Try `--workers 2`, monitor I/O, increase only if headroom exists |
| I/O latency spikes during parallel vacuum | Reduce `--workers` or return to 1; parallel vacuum is saturating storage |
| Multiple instances running concurrently | Normal — advisory locks enforce global `--workers` limit. Extra workers wait for slots |
| "Waiting for vacuum slot" messages | All slots held by other instances. Workers will proceed when a slot frees up |

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
| Autovacuum is vacuuming table X, script tries to vacuum X | **Skipped** (default) — script detects the active worker and moves on |
| Script is vacuuming table X, autovacuum wants table X | Autovacuum skips that table (yields to manual vacuum) |
| Both want different tables | Both run concurrently, no conflict |

By default, pg_vaccumen queries `pg_stat_activity` for autovacuum workers before vacuuming and **automatically skips** any table that autovacuum is already processing. This avoids wasting I/O by re-vacuuming a table that's already being handled. Skipped tables are reported in the summary.

Use `--no-skip-autovacuum` to disable this behavior and queue behind autovacuum instead (the old behavior).

**Bottom line**: The script is autovacuum-aware by default. It won't waste time re-vacuuming tables that autovacuum is already handling.

### Handling Large Tables and Active Autovacuum

When a few monster tables dominate the vacuum queue, they can block smaller tables from getting vacuumed. For example:

> 245 tables exceed the 50% threshold, but the top 2 are massive (2.7 TB and 16 TB). Autovacuum is already running on both. Without filtering, pg_vaccumen would queue behind autovacuum on those 2 tables and never reach the other 243.

Two features solve this:

| Feature | Flag | Default | Purpose |
|---------|------|---------|---------|
| Size filter | `--max-size <GB>` | No limit | Exclude tables larger than N GB from vacuum queue |
| Autovacuum skip | (automatic) | On | Skip tables where autovacuum is already running |

**When to use each:**

| Scenario | Recommendation |
|----------|----------------|
| A few huge tables block the queue | `--max-size 500` to skip tables over 500 GB |
| Autovacuum already handling the biggest tables | Let auto-skip handle it (default) |
| Both huge tables and active autovacuum | Use both — `--max-size 500` filters the monsters, auto-skip handles the rest |
| You want to vacuum everything, including monsters | `--no-skip-autovacuum` and omit `--max-size` |

## Parallel Workers (`--workers`)

By default, pg_vaccumen vacuums one table at a time. Use `--workers N` to vacuum multiple tables concurrently when catching up on a large backlog.

```bash
# Vacuum 3 tables at a time (use with caution)
python pg_vaccumen.py --host ... --database mydb --execute --workers 3 --limit 30
```

**WARNING: Parallel workers multiply I/O load, memory usage, and WAL generation. Do not increase workers without understanding the impact on your database.**

### Global concurrency limit (advisory locks)

`--workers N` is a **global limit**, not per-instance. All pg_vaccumen instances on the same database coordinate via PostgreSQL advisory locks. Before each VACUUM, a worker must acquire one of N numbered lock slots. If all slots are held, the worker waits and reports:

```
           Waiting for vacuum slot (2/2 in use)... tracker_state_cache
           Acquired vacuum slot 0
```

**What this means in practice:**

| Scenario | Concurrent VACUUMs |
|----------|--------------------|
| 1 instance, `--workers 2` | Up to 2 |
| 3 instances, `--workers 2` | Still only 2 — extra workers wait for slots |
| Cron overlap (previous run still going) | New run waits gracefully instead of dogpiling |

Slot status is reported before execution starts:

```
Global vacuum slots: 1/2 in use (advisory locks)
```

Advisory locks auto-release when a connection closes (including crashes), so there is no stale state to clean up.

**Important:** All instances should use the same `--workers` value. If one instance uses `--workers 2` and another uses `--workers 4`, the effective limit is the higher value (4), since the second instance can access slots 2-3 that the first doesn't contend for.

### What happens with multiple workers

- Each worker opens a separate database connection
- Each worker acquires a global advisory lock slot before executing VACUUM
- Each worker runs `VACUUM ANALYZE VERBOSE` on a different table concurrently
- PostgreSQL handles locking — different tables don't conflict
- Workers automatically skip tables already being vacuumed (by autovacuum or another worker)
- Workers detect vacuums started by other instances in real-time (live `pg_stat_activity` check per table)

### Safety checks (automatic)

The script automatically validates `--workers` before execution and reduces the count if:

| Check | Limit | What happens |
|-------|-------|--------------|
| Hard cap | 8 workers max | Values above 8 are silently reduced |
| Memory | `maintenance_work_mem` x workers < 2 GB | Reduced to fit within 2 GB total |
| Connections | Must leave 5 free connections | Reduced if `max_connections` headroom is tight |

If any check reduces workers to 1, a warning is printed and it falls back to sequential mode.

### I/O impact

**This is the primary risk.** VACUUM is I/O-intensive — it reads pages, writes frozen pages, and generates WAL. Multiple concurrent vacuums multiply all of this.

| Concern | Detail |
|---------|--------|
| **Disk throughput** | Aurora storage is network-attached. Too many concurrent vacuums can saturate I/O and slow production queries |
| **WAL generation** | More concurrent vacuums = more WAL = more replication lag to replicas and logical subscribers |
| **I/O credits** | Some Aurora instance types have burst I/O. Aggressive parallel vacuum can exhaust credits |
| **Memory** | Each VACUUM uses up to `maintenance_work_mem` (check `SHOW maintenance_work_mem`). 4 workers x 256 MB = 1 GB |

### Recommended settings

| Scenario | Workers | Why |
|----------|---------|-----|
| **Nightly maintenance (steady state)** | 1 | No need for parallelism when caught up |
| **Catching up on backlog (< 50 tables)** | 2 | Modest speedup, minimal I/O risk |
| **Catching up on backlog (100+ tables)** | 2-3 | Monitor I/O before going higher |
| **Emergency catch-up during maintenance window** | 3-4 | Only with active I/O monitoring |
| **Never** | >4 | Diminishing returns, real I/O contention risk |

### How to monitor impact

While running parallel workers, watch for:

```sql
-- I/O wait (are queries waiting for disk?)
SELECT wait_event_type, wait_event, count(*)
FROM pg_stat_activity
WHERE state = 'active'
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Replication lag (are subscribers falling behind?)
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_type = 'logical';
```

In CloudWatch, watch `ReadIOPS`, `WriteIOPS`, `ReadLatency`, and `WriteLatency` for the Aurora instance. If latency spikes, reduce workers.

## Killing a Running Vacuum

Sometimes you need to stop a vacuum in progress — maybe it's been running for hours on a huge table, I/O is spiking, or you need to run DDL. Here's what you need to know.

### Is it safe to kill a VACUUM?

**Yes.** Regular `VACUUM` (not `VACUUM FULL`) is crash-safe. PostgreSQL uses WAL for all changes, so an interrupted vacuum cannot corrupt data. However, the consequences depend on *how much work the vacuum had completed*:

| What happens on interrupt | Detail |
|---------------------------|--------|
| Dead tuple cleanup | **Preserved.** Pages already processed have dead tuples marked as reusable. This work is not lost. |
| Row freezing | **Preserved for processed pages.** Rows already frozen stay frozen. |
| `relfrozenxid` advancement | **Not preserved.** This only updates at the *end* of a complete vacuum. If interrupted, the table's age stays at the pre-vacuum value. |
| Table visibility map | **Preserved for processed pages.** Pages marked all-visible stay that way. |

**Bottom line:** Killing a vacuum wastes the *freeze progress* (the whole point of this script), but does not waste dead tuple cleanup. The table will be selected again on the next run.

### How to kill a vacuum

**Option 1: Kill the pg_vaccumen script**

```bash
# Ctrl+C in the terminal running pg_vaccumen
# Or from another terminal:
kill <pg_vaccumen_pid>
```

What happens when the script is killed:

| Signal | Python behavior | PostgreSQL behavior |
|--------|----------------|---------------------|
| `Ctrl+C` / `SIGINT` | KeyboardInterrupt raised, `with psycopg.connect()` context manager closes connection | Connection closed → backend receives cancel → in-flight VACUUM stops |
| `SIGTERM` (`kill`) | Same as above — Python handles SIGTERM, connection closed gracefully | Same — VACUUM stops when connection closes |
| `SIGKILL` (`kill -9`) | Python dies immediately, no cleanup | TCP connection stays open. VACUUM **continues running** until PostgreSQL detects the dead connection (via `tcp_keepalives_idle`, typically 2+ minutes) |

With `--workers > 1`, killing the script closes all worker connections, canceling all in-flight vacuums (except with `kill -9`, where all of them keep running until PostgreSQL times out the connections).

**Option 2: Cancel a specific vacuum in PostgreSQL**

If you want to stop one vacuum but let others continue (e.g., one huge table is dominating I/O):

```sql
-- Find the vacuum to cancel
SELECT pid, query, now() - query_start AS duration
FROM pg_stat_activity
WHERE query ILIKE 'vacuum%'
ORDER BY query_start;

-- Graceful cancel (vacuum rolls back cleanly)
SELECT pg_cancel_backend(<pid>);

-- Forceful terminate (kills the connection)
SELECT pg_terminate_backend(<pid>);
```

Prefer `pg_cancel_backend()` — it sends a cancel signal and the vacuum stops gracefully. Use `pg_terminate_backend()` only if cancel doesn't work (rare).

**Option 3: Use `--statement-timeout`**

The safest approach is to set a timeout upfront so vacuums that take too long are killed automatically:

```bash
# Kill any vacuum that takes longer than 30 minutes
python pg_vaccumen.py --host ... --execute --statement-timeout 1800
```

Tables that time out are reported as FAILED and will be retried on the next run.

### When to kill a vacuum

| Situation | Recommendation |
|-----------|----------------|
| Vacuum running for hours on a huge table | Consider letting it finish — killing means it restarts from scratch next run. Use `--statement-timeout` for future runs. |
| I/O spike affecting production queries | Cancel the vacuum. Production performance takes priority. The table will be picked up next run. |
| Need to run DDL (ALTER TABLE, etc.) | Cancel the vacuum on that specific table. DDL waits for `ShareUpdateExclusiveLock` to release. |
| Script is stuck / not progressing | Check `pg_stat_activity` — the vacuum may be waiting for a lock. Cancel it and investigate. |
| Emergency / need connections back | Kill the script. With `--workers`, each worker holds a connection. |

### When NOT to kill a vacuum

| Situation | Why not |
|-----------|---------|
| Vacuum is 80%+ done on a large table | Killing means all that freeze work is lost. `relfrozenxid` only advances when the vacuum *completes*. |
| Table is close to `autovacuum_freeze_max_age` | You need this vacuum to finish. If it's taking too long, let it run — emergency autovacuum will be worse. |
| "It's been running for an hour" | That may be normal for large tables. Check the table size first. 100+ GB tables can legitimately take hours. |

### What happens to a killed vacuum on the next run

The table will be selected again (its `relfrozenxid` age hasn't changed) and vacuum will restart from the beginning. Dead tuple cleanup from the interrupted vacuum is preserved, so the restart may be faster.

If a table consistently times out, consider:

1. Running it during a longer maintenance window without `--statement-timeout`
2. Using `--max-size` to skip it in regular runs and handle it separately
3. Investigating whether the table should be partitioned

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
| `STATEMENT_TIMEOUT` | 0 | Timeout in seconds for vacuum (0 = no timeout) |
| `MAX_SIZE` | | Skip tables larger than this size in GB (empty = no limit) |
| `NO_SKIP_AUTOVACUUM` | false | Don't skip tables being vacuumed by autovacuum |
| `CHECK_BLOAT` | false | Analyze tables for dead tuple bloat |
| `BLOAT_PCT` | 50 | Dead tuple percentage threshold for bloat analysis |
| `WORKERS` | 1 | Parallel vacuum workers — **global limit** via advisory locks |
| `EXECUTE` | false | Actually vacuum (false = dry-run) |
| `FORCE` | false | Proceed despite blockers |
| `SKIP_PREFLIGHT` | false | Skip preflight checks |
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
MAX_SIZE=500         # Skip tables over 500 GB
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

## Bloat Analysis

Regular `VACUUM` marks dead tuples as reusable but **does not shrink the table file**. After bulk deletes or heavy updates, tables accumulate "bloat" — wasted disk space that only `pg_repack` or `VACUUM FULL` can reclaim.

Use `--check-bloat` to identify tables with high dead tuple ratios:

```bash
# Check for tables with >50% dead tuples (default threshold)
python pg_vaccumen.py --host ... --database mydb --check-bloat

# Lower threshold to see more tables
python pg_vaccumen.py --host ... --database mydb --check-bloat --bloat-pct 20

# Combine with normal vacuum run
python pg_vaccumen.py --host ... --database mydb --execute --check-bloat --bloat-pct 30
```

### How it works

The script queries `pg_stat_user_tables` for the ratio of `n_dead_tup / (n_live_tup + n_dead_tup)`. Tables with more than 10,000 dead tuples exceeding the `--bloat-pct` threshold are reported. No extensions required — works on Aurora without superuser.

### Limitations

- Dead tuple counts are **estimates** from PostgreSQL's statistics collector, not exact values
- `n_dead_tup` resets after `VACUUM` or `AUTOVACUUM`, so recently vacuumed tables may show low counts even if they have file-level bloat
- For precise bloat measurement, use `pgstattuple` extension (requires `rds_superuser` on Aurora)
- This analysis identifies tables that *currently* have high dead tuple ratios — it does not measure historical bloat already reclaimed by vacuum

### When to act

| Dead % | Situation | Action |
|--------|-----------|--------|
| >50% | Table accumulating dead tuples faster than vacuum can process | Investigate write patterns, consider more aggressive autovacuum settings |
| >50% after recent vacuum | File-level bloat — dead space marked reusable but file not shrunk | Consider `pg_repack` (online) or `VACUUM FULL` (blocks writes) |
| >80% | Severe bloat | Priority candidate for `pg_repack` |

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
