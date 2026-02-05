#!/usr/bin/env python3
"""
pg_vaccumen — vacuum acumen for Aurora PostgreSQL.

Proactive vacuum maintenance for high-transaction Aurora PostgreSQL databases.
Vacuums tables BEFORE they hit autovacuum_freeze_max_age, spreading work over
nightly runs to create a steady plateau instead of emergency autovacuum spikes.

Run nightly via Jenkins or cron to keep relfrozenxid ages manageable.

Exit codes:
  0 - Success / OK
  1 - Error (connection failure, missing params, blockers without --force)
  2 - Warning threshold exceeded (tables approaching autovacuum trigger)
  3 - Critical threshold exceeded (tables at or past autovacuum trigger)
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg

# Default thresholds as percentage of autovacuum_freeze_max_age
# Goal: vacuum tables well before autovacuum kicks in
DEFAULT_TABLE_THRESHOLD_PCT = 50    # Select tables at 50%+ of autovacuum threshold
DEFAULT_WARNING_PCT = 80            # Warn when oldest table at 80% of threshold
DEFAULT_CRITICAL_PCT = 95           # Critical when oldest table at 95% of threshold


@dataclass
class PreflightResult:
    """Results from preflight checks."""
    autovacuum_freeze_max_age: int
    vacuum_freeze_min_age: int
    oldest_xid_age: int
    database_frozen_xid_age: int
    txns_per_day: int | None  # estimated from pg_stat_database stats_reset
    stats_age_hours: float | None  # how long stats have been accumulating
    stats_reset: str | None  # when stats were last reset
    total_txns: int | None  # total transactions since stats_reset
    long_running_txns: list[tuple[int, str, str, int]]  # pid, state, query, age_seconds
    replication_slots: list[tuple[str, str, int | None]]  # name, type, xmin_age
    prepared_txns: list[tuple[str, str, int]]  # gid, owner, age
    blockers: list[str]
    baseline_txns_per_day: int | None = None  # estimated from baseline file
    baseline_age_hours: float | None = None  # hours since baseline was recorded

    @property
    def headroom_to_autovacuum(self) -> int:
        """How many XIDs until autovacuum kicks in."""
        return self.autovacuum_freeze_max_age - self.oldest_xid_age

    @property
    def pct_to_autovacuum(self) -> float:
        """Percentage of autovacuum threshold consumed."""
        return (self.oldest_xid_age / self.autovacuum_freeze_max_age) * 100

    @property
    def effective_txns_per_day(self) -> int | None:
        """Best available transaction rate estimate."""
        return self.txns_per_day or self.baseline_txns_per_day

    @property
    def days_until_autovacuum(self) -> float | None:
        """Estimated days until oldest table hits autovacuum threshold."""
        rate = self.effective_txns_per_day
        if rate and rate > 0:
            return self.headroom_to_autovacuum / rate
        return None


@dataclass
class AlertStatus:
    """Alert level based on thresholds."""
    level: str  # "ok", "warning", "critical"
    exit_code: int
    message: str


def get_cluster_endpoint(cluster: str, region: str) -> str:
    """Get the writer endpoint for an Aurora cluster."""
    rds = boto3.client("rds", region_name=region)
    response = rds.describe_db_clusters(DBClusterIdentifier=cluster)
    return response["DBClusters"][0]["Endpoint"]


def get_password_from_secrets_manager(cluster: str, region: str) -> str | None:
    """Retrieve password from AWS Secrets Manager if cluster has MasterUserSecret."""
    rds = boto3.client("rds", region_name=region)
    response = rds.describe_db_clusters(DBClusterIdentifier=cluster)
    cluster_info = response["DBClusters"][0]

    if not cluster_info.get("MasterUserSecret"):
        return None

    secret_arn = cluster_info["MasterUserSecret"]["SecretArn"]
    secrets = boto3.client("secretsmanager", region_name=region)
    secret = secrets.get_secret_value(SecretId=secret_arn)
    return json.loads(secret["SecretString"]).get("password")


@dataclass
class Baseline:
    """Stored baseline for transaction rate calculation."""
    timestamp: str
    total_txns: int

    def hours_since(self) -> float:
        """Hours elapsed since baseline was recorded."""
        baseline_dt = datetime.fromisoformat(self.timestamp)
        now = datetime.now(timezone.utc)
        # Handle both timezone-aware and naive datetimes
        if baseline_dt.tzinfo is None:
            baseline_dt = baseline_dt.replace(tzinfo=timezone.utc)
        return (now - baseline_dt).total_seconds() / 3600


def load_baseline(path: Path) -> Baseline | None:
    """Load baseline from file if it exists."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return Baseline(timestamp=data["timestamp"], total_txns=data["total_txns"])
    except (json.JSONDecodeError, KeyError):
        return None


def save_baseline(path: Path, total_txns: int) -> None:
    """Save current stats as baseline."""
    data = {
        "timestamp": datetime.now().isoformat(),
        "total_txns": total_txns,
    }
    path.write_text(json.dumps(data, indent=2))


def run_preflight_checks(conn: psycopg.Connection, database: str) -> PreflightResult:
    """Run preflight checks and return results."""
    with conn.cursor() as cur:
        # Get autovacuum settings
        cur.execute("show autovacuum_freeze_max_age")
        autovacuum_freeze_max_age = int(cur.fetchone()[0])

        cur.execute("show vacuum_freeze_min_age")
        vacuum_freeze_min_age = int(cur.fetchone()[0])

        # Get oldest relfrozenxid age across all tables
        cur.execute("""
            select max(age(relfrozenxid))
            from pg_class
            where relkind in ('r', 't')
        """)
        oldest_xid_age = cur.fetchone()[0] or 0

        # Get database frozen xid age
        cur.execute("""
            select age(datfrozenxid)
            from pg_database
            where datname = %s
        """, (database,))
        database_frozen_xid_age = cur.fetchone()[0] or 0

        # Get transaction rate from pg_stat_database
        cur.execute("""
            select xact_commit + xact_rollback as total_txns,
                   extract(epoch from (now() - stats_reset)) / 3600 as stats_age_hours,
                   stats_reset::text
            from pg_stat_database
            where datname = %s
        """, (database,))
        row = cur.fetchone()
        txns_per_day = None
        stats_age_hours = None
        stats_reset = None
        total_txns = None
        if row:
            total_txns = row[0]
            stats_reset = row[2]
            if row[0] and row[1] and row[1] > 1:  # need at least 1 hour of stats
                stats_age_hours = float(row[1])
                txns_per_hour = total_txns / stats_age_hours
                txns_per_day = int(txns_per_hour * 24)

        # Check for long-running transactions (older than 1 hour)
        cur.execute("""
            select pid, state, left(query, 50) as query,
                   extract(epoch from (now() - xact_start))::int as age_seconds
            from pg_stat_activity
            where xact_start is not null
              and state != 'idle'
              and pid != pg_backend_pid()
              and xact_start < now() - interval '1 hour'
            order by xact_start
            limit 10
        """)
        long_running_txns = cur.fetchall()

        # Check replication slots
        cur.execute("""
            select slot_name, slot_type,
                   case when xmin is not null then age(xmin) else null end as xmin_age
            from pg_replication_slots
            order by xmin_age desc nulls last
        """)
        replication_slots = cur.fetchall()

        # Check prepared transactions
        cur.execute("""
            select gid, owner, age(transaction)
            from pg_prepared_xacts
            order by age(transaction) desc
        """)
        prepared_txns = cur.fetchall()

        # Determine blockers
        blockers = []
        if long_running_txns:
            blockers.append(f"{len(long_running_txns)} long-running transaction(s) (>1 hour)")

        slots_with_old_xmin = [s for s in replication_slots if s[2] and s[2] > 100_000_000]
        if slots_with_old_xmin:
            blockers.append(f"{len(slots_with_old_xmin)} replication slot(s) with old xmin (>100M)")

        if prepared_txns:
            blockers.append(f"{len(prepared_txns)} prepared transaction(s)")

        return PreflightResult(
            autovacuum_freeze_max_age=autovacuum_freeze_max_age,
            vacuum_freeze_min_age=vacuum_freeze_min_age,
            oldest_xid_age=oldest_xid_age,
            database_frozen_xid_age=database_frozen_xid_age,
            txns_per_day=txns_per_day,
            stats_age_hours=stats_age_hours,
            stats_reset=stats_reset,
            total_txns=total_txns,
            long_running_txns=long_running_txns,
            replication_slots=replication_slots,
            prepared_txns=prepared_txns,
            blockers=blockers,
        )


def print_preflight_report(pf: PreflightResult) -> None:
    """Print preflight check results."""
    print("=" * 70)
    print("PREFLIGHT CHECKS")
    print("=" * 70)
    print()

    # Settings
    print("Autovacuum Settings:")
    print(f"  autovacuum_freeze_max_age: {pf.autovacuum_freeze_max_age:,}")
    print(f"  vacuum_freeze_min_age:     {pf.vacuum_freeze_min_age:,}")
    print()

    # XID Age Status - focus on distance to autovacuum trigger
    print("Transaction ID Age (relative to autovacuum trigger):")
    print(f"  Oldest table relfrozenxid age: {pf.oldest_xid_age:,}")
    print(f"  Autovacuum trigger point:      {pf.autovacuum_freeze_max_age:,}")
    print(f"  Headroom until autovacuum:     {pf.headroom_to_autovacuum:,}")
    print(f"  Percent of threshold used:     {pf.pct_to_autovacuum:.1f}%")
    print()
    print(f"  Database datfrozenxid age:     {pf.database_frozen_xid_age:,}")
    print()

    # Transaction rate - critical for capacity planning
    print("Transaction Rate:")
    if pf.stats_reset:
        print(f"  Stats reset at:                {pf.stats_reset}")
        if pf.stats_age_hours:
            stats_days = pf.stats_age_hours / 24
            print(f"  Stats accumulating for:        {stats_days:.1f} days")
    else:
        print(f"  Stats reset at:                (never)")
    if pf.total_txns:
        print(f"  Total transactions recorded:   {pf.total_txns:,}")
    if pf.txns_per_day:
        print(f"  Transactions per day:          {pf.txns_per_day:,} (from pg_stat_database)")
    if pf.baseline_txns_per_day:
        baseline_days = pf.baseline_age_hours / 24 if pf.baseline_age_hours else 0
        print(f"  Transactions per day:          {pf.baseline_txns_per_day:,} (from baseline, {baseline_days:.1f} days)")
    if pf.effective_txns_per_day:
        if pf.days_until_autovacuum is not None:
            if pf.days_until_autovacuum < 1:
                hours = pf.days_until_autovacuum * 24
                print(f"  Est. time to autovacuum:       {hours:.1f} hours  *** URGENT ***")
            elif pf.days_until_autovacuum < 7:
                print(f"  Est. time to autovacuum:       {pf.days_until_autovacuum:.1f} days  ** WARNING **")
            else:
                print(f"  Est. time to autovacuum:       {pf.days_until_autovacuum:.1f} days")
    elif not pf.stats_reset and not pf.baseline_txns_per_day:
        print("  Transactions per day:          (use --baseline-to-db or --baseline-file to track rate)")
    elif not pf.stats_reset:
        print("  Transactions per day:          (waiting for baseline data, <1 hour)")
    else:
        print("  Transactions per day:          (<1 hour since reset, waiting for data)")
    print()

    # Long-running transactions
    print(f"Long-Running Transactions (>1 hour): {len(pf.long_running_txns)}")
    if pf.long_running_txns:
        print("-" * 70)
        print(f"  {'PID':<10} {'State':<15} {'Age':<12} Query")
        print("-" * 70)
        for pid, state, query, age_secs in pf.long_running_txns:
            hours = age_secs // 3600
            mins = (age_secs % 3600) // 60
            print(f"  {pid:<10} {state:<15} {hours}h {mins}m      {query}...")
    print()

    # Replication slots
    print(f"Replication Slots: {len(pf.replication_slots)}")
    if pf.replication_slots:
        print("-" * 70)
        print(f"  {'Slot Name':<30} {'Type':<15} {'xmin Age':>15}")
        print("-" * 70)
        for name, slot_type, xmin_age in pf.replication_slots:
            xmin_str = f"{xmin_age:,}" if xmin_age else "N/A"
            print(f"  {name:<30} {slot_type:<15} {xmin_str:>15}")
    print()

    # Prepared transactions
    print(f"Prepared Transactions: {len(pf.prepared_txns)}")
    if pf.prepared_txns:
        print("-" * 70)
        print(f"  {'GID':<30} {'Owner':<20} {'Age':>15}")
        print("-" * 70)
        for gid, owner, age in pf.prepared_txns:
            print(f"  {gid:<30} {owner:<20} {age:>15,}")
    print()

    # Blockers summary
    if pf.blockers:
        print("BLOCKERS DETECTED:")
        for blocker in pf.blockers:
            print(f"  - {blocker}")
        print()
    else:
        print("No blockers detected.")
        print()

    print("=" * 70)
    print()


def generate_recommendations(
    pf: PreflightResult,
    tables_found: int,
    tables_total: int,
    tables_limit: int,
    threshold: int,
) -> list[str]:
    """Generate tuning recommendations based on current state."""
    recommendations = []
    tables_waiting = tables_total - tables_found

    # Recommendations based on time to autovacuum
    if pf.days_until_autovacuum is not None:
        if pf.days_until_autovacuum < 1:
            recommendations.append(
                f"URGENT: Only {pf.days_until_autovacuum * 24:.1f} hours until autovacuum triggers. "
                f"Increase --limit significantly (try {tables_limit * 4}) or run more frequently."
            )
        elif pf.days_until_autovacuum < 3:
            recommendations.append(
                f"Time to autovacuum is {pf.days_until_autovacuum:.1f} days. "
                f"Consider increasing --limit to {tables_limit * 2} or running twice daily."
            )
        elif pf.days_until_autovacuum < 7:
            recommendations.append(
                f"Time to autovacuum is {pf.days_until_autovacuum:.1f} days. "
                f"Monitor closely; may need to increase --limit if trend continues."
            )
        elif pf.days_until_autovacuum > 30 and tables_found == 0:
            recommendations.append(
                f"Time to autovacuum is {pf.days_until_autovacuum:.0f} days with no tables to vacuum. "
                f"Maintenance is well ahead of transaction load. Could reduce frequency or raise --threshold."
            )

    # Recommendations based on table counts - backlog awareness
    if tables_waiting > 0:
        suggested_limit = min(tables_total, tables_limit * 3)  # cap at 3x
        recommendations.append(
            f"Backlog: {tables_waiting} additional table(s) waiting beyond --limit of {tables_limit}. "
            f"Total needing vacuum: {tables_total}. Consider --limit {suggested_limit}."
        )

    # Recommendations based on threshold vs current ages
    threshold_pct = threshold * 100 // pf.autovacuum_freeze_max_age
    if pf.pct_to_autovacuum > 90:
        recommendations.append(
            f"Oldest table is at {pf.pct_to_autovacuum:.0f}% of autovacuum threshold. "
            f"Emergency autovacuum is imminent. Run with --execute immediately."
        )
    elif threshold_pct > 70 and pf.pct_to_autovacuum > threshold_pct:
        recommendations.append(
            f"Table selection threshold ({threshold_pct}%) is high and tables are exceeding it. "
            f"Consider lowering threshold to catch tables earlier."
        )

    return recommendations


def evaluate_alert_status(
    oldest_xid_age: int,
    autovacuum_freeze_max_age: int,
    warning_pct: int,
    critical_pct: int,
) -> AlertStatus:
    """Evaluate alert status based on proximity to autovacuum threshold."""
    pct_used = (oldest_xid_age / autovacuum_freeze_max_age) * 100
    warning_threshold = int(autovacuum_freeze_max_age * warning_pct / 100)
    critical_threshold = int(autovacuum_freeze_max_age * critical_pct / 100)

    if oldest_xid_age >= critical_threshold:
        return AlertStatus(
            level="critical",
            exit_code=3,
            message=f"CRITICAL: Oldest XID age at {pct_used:.0f}% of autovacuum threshold ({oldest_xid_age:,} / {autovacuum_freeze_max_age:,})",
        )
    elif oldest_xid_age >= warning_threshold:
        return AlertStatus(
            level="warning",
            exit_code=2,
            message=f"WARNING: Oldest XID age at {pct_used:.0f}% of autovacuum threshold ({oldest_xid_age:,} / {autovacuum_freeze_max_age:,})",
        )
    else:
        return AlertStatus(
            level="ok",
            exit_code=0,
            message=f"OK: Oldest XID age at {pct_used:.0f}% of autovacuum threshold ({oldest_xid_age:,} / {autovacuum_freeze_max_age:,})",
        )


def get_tables_to_vacuum(
    conn: psycopg.Connection,
    threshold: int,
    limit: int,
) -> tuple[list[tuple[str, int]], int]:
    """Query tables with relfrozenxid age exceeding threshold.

    Returns:
        Tuple of (limited list of tables, total count of tables exceeding threshold)
    """
    # Get total count first
    count_query = """
        select count(*)
        from pg_class
        where relkind in ('r', 't')
          and age(relfrozenxid) > %s
    """
    # Get limited results
    query = """
        select oid::regclass::text, age(relfrozenxid)
        from pg_class
        where relkind in ('r', 't')
          and age(relfrozenxid) > %s
        order by age(relfrozenxid) desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(count_query, (threshold,))
        total_count = cur.fetchone()[0]

        cur.execute(query, (threshold, limit))
        tables = cur.fetchall()

        return tables, total_count


@dataclass
class VacuumMetric:
    """Metrics from a single vacuum operation."""
    timestamp: str
    table: str
    age_before: int
    size_bytes: int
    duration_seconds: float
    database: str
    host: str


def get_table_size(conn: psycopg.Connection, table: str) -> int:
    """Get table size in bytes."""
    with conn.cursor() as cur:
        cur.execute("select pg_total_relation_size(%s)", (table,))
        return cur.fetchone()[0] or 0


def vacuum_table(conn: psycopg.Connection, table: str) -> float:
    """Run VACUUM ANALYZE VERBOSE on a single table.

    Returns:
        Duration in seconds.
    """
    conn.rollback()  # End any implicit transaction before setting autocommit
    conn.autocommit = True
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(f"vacuum (verbose, analyze) {table}")
    duration = time.perf_counter() - start
    conn.autocommit = False
    return duration


def load_metrics(path: Path) -> list[dict]:
    """Load existing metrics from file."""
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, KeyError):
        return []


def save_metrics(path: Path, metrics: list[VacuumMetric]) -> None:
    """Append metrics to file."""
    existing = load_metrics(path)
    existing.extend([asdict(m) for m in metrics])
    path.write_text(json.dumps(existing, indent=2))


def ensure_metrics_table(conn: psycopg.Connection) -> None:
    """Create vacuum_metrics table if it doesn't exist."""
    conn.rollback()  # End any implicit transaction before setting autocommit
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            create table if not exists vacuum_metrics (
                id serial primary key,
                vacuumed_at timestamptz default now(),
                table_name text not null,
                age_before bigint,
                size_bytes bigint,
                duration_seconds numeric(10,2),
                database_name text,
                cluster_host text
            )
        """)
        cur.execute("""
            create index if not exists idx_vacuum_metrics_table_time
            on vacuum_metrics (table_name, vacuumed_at)
        """)
    conn.autocommit = False


def ensure_baseline_table(conn: psycopg.Connection) -> None:
    """Create vacuum_baseline table if it doesn't exist."""
    conn.rollback()  # End any implicit transaction before setting autocommit
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            create table if not exists vacuum_baseline (
                id serial primary key,
                recorded_at timestamptz default now(),
                total_txns bigint not null,
                database_name text not null,
                cluster_host text,
                unique (database_name)
            )
        """)
    conn.autocommit = False


def load_baseline_from_db(conn: psycopg.Connection, database: str) -> Baseline | None:
    """Load baseline from database."""
    with conn.cursor() as cur:
        cur.execute("""
            select recorded_at::text, total_txns
            from vacuum_baseline
            where database_name = %s
        """, (database,))
        row = cur.fetchone()
        if row:
            return Baseline(timestamp=row[0], total_txns=row[1])
    return None


def save_baseline_to_db(conn: psycopg.Connection, total_txns: int, database: str, host: str) -> None:
    """Save baseline to database (upsert)."""
    with conn.cursor() as cur:
        cur.execute("""
            insert into vacuum_baseline (recorded_at, total_txns, database_name, cluster_host)
            values (now(), %s, %s, %s)
            on conflict (database_name)
            do update set recorded_at = now(), total_txns = excluded.total_txns, cluster_host = excluded.cluster_host
        """, (total_txns, database, host))
    conn.commit()


def save_metrics_to_db(conn: psycopg.Connection, metrics: list[VacuumMetric]) -> None:
    """Insert metrics into vacuum_metrics table."""
    with conn.cursor() as cur:
        for m in metrics:
            cur.execute("""
                insert into vacuum_metrics
                    (vacuumed_at, table_name, age_before, size_bytes,
                     duration_seconds, database_name, cluster_host)
                values (%s, %s, %s, %s, %s, %s, %s)
            """, (
                m.timestamp,
                m.table,
                m.age_before,
                m.size_bytes,
                m.duration_seconds,
                m.database,
                m.host,
            ))
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Proactive vacuum maintenance for high-transaction Aurora PostgreSQL databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Proactive Maintenance Strategy:
  This script vacuums tables BEFORE they hit autovacuum_freeze_max_age,
  spreading vacuum work over nightly runs instead of emergency spikes.

  Default behavior (with 200M autovacuum_freeze_max_age):
    --threshold 100M    Select tables at 50%+ of autovacuum threshold
    --warning-pct 80    Warn when oldest table at 160M (80%)
    --critical-pct 95   Critical when oldest table at 190M (95%)

Exit codes:
  0 - Success / OK
  1 - Error (connection failure, blockers without --force)
  2 - Warning: tables approaching autovacuum trigger
  3 - Critical: tables at or past autovacuum trigger
        """,
    )
    parser.add_argument("--cluster", help="Aurora cluster identifier (required unless --host specified)")
    parser.add_argument("--host", help="Database host (bypasses AWS cluster lookup)")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument("--db-username", default="postgres", help="Database username")
    parser.add_argument("--db-password", help="Database password (required when using --host)")
    parser.add_argument("--limit", type=int, default=10, help="Max tables to vacuum per run")
    parser.add_argument(
        "--statement-timeout",
        type=int,
        default=0,
        help="Statement timeout in seconds for vacuum operations (0 = no timeout, default: 0)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        help="relfrozenxid age threshold for table selection (default: 50%% of autovacuum_freeze_max_age)",
    )
    parser.add_argument(
        "--warning-pct",
        type=int,
        default=DEFAULT_WARNING_PCT,
        help=f"Alert warning at this %% of autovacuum_freeze_max_age (default: {DEFAULT_WARNING_PCT}%%)",
    )
    parser.add_argument(
        "--critical-pct",
        type=int,
        default=DEFAULT_CRITICAL_PCT,
        help=f"Alert critical at this %% of autovacuum_freeze_max_age (default: {DEFAULT_CRITICAL_PCT}%%)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute vacuum (default is dry-run mode)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed even if blockers are detected",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip preflight checks (not recommended)",
    )
    parser.add_argument(
        "--baseline-file",
        type=Path,
        help="File to store/load transaction baseline for rate calculation",
    )
    parser.add_argument(
        "--save-baseline",
        action="store_true",
        help="Save current stats as new baseline (overwrites existing)",
    )
    parser.add_argument(
        "--metrics-file",
        type=Path,
        help="JSON file to append vacuum metrics (duration, size, etc.)",
    )
    parser.add_argument(
        "--metrics-to-db",
        action="store_true",
        help="Store vacuum metrics in vacuum_metrics table",
    )
    parser.add_argument(
        "--baseline-to-db",
        action="store_true",
        help="Store/load transaction baseline from vacuum_baseline table",
    )
    args = parser.parse_args()

    # Validate arguments
    if not args.host and not args.cluster:
        print("ERROR: Either --cluster or --host must be specified", file=sys.stderr)
        return 1

    # Get connection details
    if args.host:
        endpoint = args.host
        password = args.db_password
        if not password:
            print("ERROR: --db-password is required when using --host", file=sys.stderr)
            return 1
    else:
        endpoint = get_cluster_endpoint(args.cluster, args.region)
        password = args.db_password or get_password_from_secrets_manager(args.cluster, args.region)
        if not password:
            print("ERROR: No password provided and cluster has no MasterUserSecret", file=sys.stderr)
            return 1

    print(f"Cluster: {args.cluster or '(direct connection)'}")
    print(f"Endpoint: {endpoint}")
    print(f"Database: {args.database}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print()

    with psycopg.connect(
        host=endpoint,
        dbname=args.database,
        user=args.db_username,
        password=password,
        sslmode="prefer",
    ) as conn:
        # Set statement timeout if specified (converts seconds to milliseconds)
        if args.statement_timeout > 0:
            with conn.cursor() as cur:
                cur.execute(f"set statement_timeout = {args.statement_timeout * 1000}")
            print(f"Statement timeout: {args.statement_timeout}s")

        # Run preflight checks (always needed to get autovacuum_freeze_max_age)
        preflight = run_preflight_checks(conn, args.database)

        # Handle baseline for transaction rate calculation
        if args.baseline_to_db:
            ensure_baseline_table(conn)
            if args.save_baseline:
                if preflight.total_txns:
                    save_baseline_to_db(conn, preflight.total_txns, args.database, endpoint)
                    print(f"Baseline saved to vacuum_baseline table")
                else:
                    print("WARNING: Cannot save baseline - no transaction count available")
            else:
                baseline = load_baseline_from_db(conn, args.database)
                if baseline and preflight.total_txns:
                    hours = baseline.hours_since()
                    if hours >= 1:  # need at least 1 hour
                        txn_delta = preflight.total_txns - baseline.total_txns
                        if txn_delta > 0:
                            preflight.baseline_txns_per_day = int((txn_delta / hours) * 24)
                            preflight.baseline_age_hours = hours
            print()
        elif args.baseline_file:
            if args.save_baseline:
                if preflight.total_txns:
                    save_baseline(args.baseline_file, preflight.total_txns)
                    print(f"Baseline saved to {args.baseline_file}")
                else:
                    print("WARNING: Cannot save baseline - no transaction count available")
            else:
                baseline = load_baseline(args.baseline_file)
                if baseline and preflight.total_txns:
                    hours = baseline.hours_since()
                    if hours >= 1:  # need at least 1 hour
                        txn_delta = preflight.total_txns - baseline.total_txns
                        if txn_delta > 0:
                            preflight.baseline_txns_per_day = int((txn_delta / hours) * 24)
                            preflight.baseline_age_hours = hours
            print()

        # Calculate threshold based on autovacuum_freeze_max_age if not specified
        threshold = args.threshold
        if threshold is None:
            threshold = int(preflight.autovacuum_freeze_max_age * DEFAULT_TABLE_THRESHOLD_PCT / 100)

        print(f"Autovacuum freeze max age: {preflight.autovacuum_freeze_max_age:,}")
        print(f"Table selection threshold: {threshold:,} ({threshold * 100 // preflight.autovacuum_freeze_max_age}% of max)")
        print(f"Warning at: {args.warning_pct}% ({int(preflight.autovacuum_freeze_max_age * args.warning_pct / 100):,})")
        print(f"Critical at: {args.critical_pct}% ({int(preflight.autovacuum_freeze_max_age * args.critical_pct / 100):,})")
        print(f"Max tables per run: {args.limit}")
        print()

        if not args.skip_preflight:
            print_preflight_report(preflight)

            # Check for blockers
            if preflight.blockers and not args.force:
                print("ERROR: Blockers detected. Use --force to proceed anyway.", file=sys.stderr)
                print("       Vacuum may not be able to advance relfrozenxid with active blockers.", file=sys.stderr)
                return 1

            if preflight.blockers and args.force:
                print("WARNING: Proceeding despite blockers (--force specified)")
                print()

        # Evaluate alert status
        alert = evaluate_alert_status(
            preflight.oldest_xid_age,
            preflight.autovacuum_freeze_max_age,
            args.warning_pct,
            args.critical_pct,
        )
        print(f"Alert Status: {alert.message}")
        print()

        # Get tables to vacuum
        tables, total_count = get_tables_to_vacuum(conn, threshold, args.limit)

        if not tables:
            print(f"No tables exceed threshold ({threshold:,})")
            print("Proactive maintenance is keeping up with transaction load.")

            # Still show recommendations even with no tables
            recommendations = generate_recommendations(preflight, 0, 0, args.limit, threshold)
            if recommendations:
                print()
                print("=" * 80)
                print("RECOMMENDATIONS")
                print("=" * 80)
                for rec in recommendations:
                    print(f"  • {rec}")
                print()

            return alert.exit_code

        # Show table count with backlog info
        if total_count > len(tables):
            print(f"Tables to vacuum ({len(tables)} of {total_count} total exceeding threshold):")
        else:
            print(f"Tables to vacuum ({len(tables)}):")
        print("-" * 80)
        print(f"{'Table':<45} {'Age':>15} {'% of Max':>12}")
        print("-" * 80)
        for table, age in tables:
            pct = age * 100 // preflight.autovacuum_freeze_max_age
            print(f"{table:<45} {age:>15,} {pct:>11}%")
        print("-" * 80)
        if total_count > len(tables):
            print(f"  ... and {total_count - len(tables)} more table(s) waiting")
        print()

        # Show recommendations
        recommendations = generate_recommendations(preflight, len(tables), total_count, args.limit, threshold)
        if recommendations:
            print("=" * 80)
            print("RECOMMENDATIONS")
            print("=" * 80)
            for rec in recommendations:
                print(f"  • {rec}")
            print()

        if not args.execute:
            print("DRY-RUN: No vacuum performed. Use --execute to run.")
            return alert.exit_code

        # Ensure metrics table exists if saving to DB
        if args.metrics_to_db:
            ensure_metrics_table(conn)

        print("Executing vacuum...")
        print("=" * 80)
        metrics: list[VacuumMetric] = []
        total_duration = 0.0
        collect_metrics = args.metrics_file or args.metrics_to_db

        failed_tables: list[tuple[str, str]] = []

        for i, (table, age) in enumerate(tables, 1):
            pct = age * 100 // preflight.autovacuum_freeze_max_age
            size_bytes = get_table_size(conn, table)
            size_mb = size_bytes / (1024 * 1024)

            print(f"\n[{i}/{len(tables)}] VACUUM ANALYZE VERBOSE {table}")
            print(f"           (age={age:,}, {pct}% of max, {size_mb:.1f} MB)")
            print("-" * 80)

            try:
                duration = vacuum_table(conn, table)
                total_duration += duration
                print(f"           Completed in {duration:.1f}s")

                if collect_metrics:
                    metrics.append(VacuumMetric(
                        timestamp=datetime.now().isoformat(),
                        table=table,
                        age_before=age,
                        size_bytes=size_bytes,
                        duration_seconds=round(duration, 2),
                        database=args.database,
                        host=endpoint,
                    ))
            except Exception as e:
                print(f"           FAILED: {e}")
                failed_tables.append((table, str(e)))

        print()
        print("=" * 80)
        successful = len(tables) - len(failed_tables)
        print(f"Vacuum complete: {successful}/{len(tables)} tables in {total_duration:.1f}s")

        if failed_tables:
            print()
            print("FAILED TABLES:")
            for table, error in failed_tables:
                print(f"  - {table}: {error}")

        if args.metrics_file and metrics:
            save_metrics(args.metrics_file, metrics)
            print(f"Metrics appended to {args.metrics_file}")

        if args.metrics_to_db and metrics:
            save_metrics_to_db(conn, metrics)
            print(f"Metrics saved to vacuum_metrics table ({len(metrics)} rows)")

        # Return error exit code if any tables failed
        if failed_tables:
            return 1

        return alert.exit_code


if __name__ == "__main__":
    sys.exit(main())
