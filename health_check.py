#!/usr/bin/env python3
import sys
import psycopg2
import datetime

DB_DSN = "dbname=igrf user=postgres password=1234 host=host.docker.internal port=5434"
SCHEMA = 'public'
DATE_CANDIDATES = ['date', 'datetime', 'dt', 'time_tag']
STATION_CANDIDATES = ['station_id', 'magstation_id', 'iaga_code', 'name', 'id', 'channel']
EVENT_START_CANDIDATES = ['start', 'begin']
EVENT_END_CANDIDATES = ['end', 'stop', 'finish']

def get_tables(conn, schema=SCHEMA):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        return [row[0] for row in cur.fetchall()]

def get_columns(conn, table, schema=SCHEMA):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return [row[0] for row in cur.fetchall()]

def get_max_date(conn, table, date_col, where=None, where_val=None, schema=SCHEMA):
    with conn.cursor() as cur:
        if where:
            cur.execute(
                f'SELECT MAX("{date_col}") FROM "{schema}"."{table}" WHERE "{where}" = %s',
                (where_val,)
            )
        else:
            cur.execute(
                f'SELECT MAX("{date_col}") FROM "{schema}"."{table}"'
            )
        result = cur.fetchone()
        return result[0] if result else None

def get_event_intervals(conn, table, start_col, end_col, schema=SCHEMA):
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT "{start_col}", "{end_col}" FROM "{schema}"."{table}" WHERE "{start_col}" IS NOT NULL AND "{end_col}" IS NOT NULL ORDER BY "{start_col}"'
        )
        return [(row[0], row[1]) for row in cur.fetchall()]

def find_min_interval(timestamps):
    if len(timestamps) < 2:
        return None
    intervals = [(timestamps[i] - timestamps[i - 1]).total_seconds() for i in range(1, len(timestamps))]
    intervals = [iv for iv in intervals if iv > 0]
    if not intervals:
        return None
    intervals.sort()
    mid = len(intervals) // 2
    if len(intervals) % 2 == 0:
        return (intervals[mid - 1] + intervals[mid]) / 2
    else:
        return intervals[mid]

def group_gaps(gaps):
    if not gaps:
        return []
    grouped = []
    start, end, total = gaps[0]
    for i in range(1, len(gaps)):
        prev_end = gaps[i - 1][1]
        curr_start, curr_end, curr_gap = gaps[i]
        if curr_start == prev_end:
            end = curr_end
            total += curr_gap
        else:
            grouped.append((start, end, total))
            start, end, total = curr_start, curr_end, curr_gap
    grouped.append((start, end, total))
    return grouped

def analyze_event_table(conn, table, start_col, end_col):
    intervals = get_event_intervals(conn, table, start_col, end_col)
    if len(intervals) < 2:
        return f"{table} - недостаточно данных для анализа.\n"

    gaps = []
    for i in range(1, len(intervals)):
        prev_end = intervals[i - 1][1]
        curr_start = intervals[i][0]
        if prev_end is None or curr_start is None:
            continue
        diff = curr_start - prev_end
        if diff.total_seconds() > 0:
            gaps.append((prev_end, curr_start, diff))

    max_date = max([i[1] for i in intervals if i[1] is not None], default=None)

    if not gaps:
        return f"{table} - пропусков нет. Последняя дата: {max_date}\n"

    intervals_sorted = sorted([g[2].total_seconds() for g in gaps])
    if len(intervals_sorted) < 3:
        min_interval = min(intervals_sorted)
    else:
        mid = len(intervals_sorted) // 2
        if len(intervals_sorted) % 2 == 0:
            min_interval = (intervals_sorted[mid - 1] + intervals_sorted[mid]) / 2
        else:
            min_interval = intervals_sorted[mid]

    factor = 3
    threshold = min_interval * factor
    significant_gaps = [g for g in gaps if g[2].total_seconds() > threshold]

    if not significant_gaps:
        return f"{table} - пропусков нет. Последняя дата: {max_date}\n"

    grouped = group_gaps(significant_gaps)
    ranges_str = ", ".join(
        f"({start.strftime('%d.%m.%Y')}-{end.strftime('%d.%m.%Y')})" for start, end, _ in grouped
    )
    return f"{table} - есть пропуски {ranges_str}. Последняя дата: {max_date}\n"

def get_distinct(conn, table, col, schema=SCHEMA):
    with conn.cursor() as cur:
        cur.execute(f'SELECT DISTINCT "{col}" FROM "{schema}"."{table}"')
        return [row[0] for row in cur.fetchall()]

def get_timestamps(conn, table, date_col, where=None, where_val=None, schema=SCHEMA):
    with conn.cursor() as cur:
        if where:
            cur.execute(
                f'SELECT "{date_col}" FROM "{schema}"."{table}" WHERE "{where}" = %s AND "{date_col}" IS NOT NULL ORDER BY "{date_col}"',
                (where_val,))
        else:
            cur.execute(
                f'SELECT "{date_col}" FROM "{schema}"."{table}" WHERE "{date_col}" IS NOT NULL ORDER BY "{date_col}"'
            )
        return [row[0] for row in cur.fetchall()]

def find_significant_gaps(timestamps, min_interval, factor=3):
    significant_gaps = []
    threshold = min_interval * factor
    for i in range(1, len(timestamps)):
        diff = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if diff > threshold:
            significant_gaps.append((timestamps[i - 1], timestamps[i], datetime.timedelta(seconds=diff)))
    return significant_gaps

def analyze_time_series(conn, table, date_col, station_col=None):
    all_gaps = []

    max_date_overall = None

    if station_col:
        station_ids = get_distinct(conn, table, station_col)
        for station_id in station_ids:
            timestamps = get_timestamps(conn, table, date_col, where=station_col, where_val=station_id)
            timestamps = sorted(set(timestamps))
            if len(timestamps) < 2:
                continue
            min_interval = find_min_interval(timestamps)
            if min_interval is None:
                continue
            gaps = find_significant_gaps(timestamps, min_interval, factor=3)
            grouped_gaps = group_gaps(gaps)
            all_gaps.extend(grouped_gaps)

            max_station_date = max(timestamps) if timestamps else None
            if max_station_date and (max_date_overall is None or max_station_date > max_date_overall):
                max_date_overall = max_station_date
    else:
        timestamps = get_timestamps(conn, table, date_col)
        timestamps = sorted(set(timestamps))
        if len(timestamps) < 2:
            max_date = max(timestamps) if timestamps else None
            return f"{table} - недостаточно данных для анализа. Последняя дата: {max_date}\n"
        min_interval = find_min_interval(timestamps)
        if min_interval is None:
            max_date = max(timestamps) if timestamps else None
            return f"{table} - не удалось вычислить минимальный интервал. Последняя дата: {max_date}\n"
        gaps = find_significant_gaps(timestamps, min_interval, factor=3)
        grouped_gaps = group_gaps(gaps)
        all_gaps.extend(grouped_gaps)
        max_date_overall = max(timestamps) if timestamps else None

    if not all_gaps:
        return f"{table} - пропусков нет. Последняя дата: {max_date_overall}\n"

    all_gaps_sorted = sorted(all_gaps, key=lambda x: x[0])
    grouped_all = group_gaps(all_gaps_sorted)
    ranges_str = ", ".join(
        f"({start.strftime('%d.%m.%Y')}-{end.strftime('%d.%m.%Y')})" for start, end, _ in grouped_all
    )
    return f"{table} - есть пропуски {ranges_str}. Последняя дата: {max_date_overall}\n"

def check_db_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
        return "[INFO] Подключение к БД: OK"
    except Exception as e:
        return f"[ERROR] Ошибка подключения к БД: {e}"

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Проверка целостности данных")
    parser.add_argument('command', choices=['dbconn', 'data'], help="Команда проверки")
    args = parser.parse_args()

    output = ""
    conn = None

    try:
        conn = psycopg2.connect(DB_DSN)
    except Exception as e:
        print(f"[ERROR] Ошибка подключения к БД: {e}")
        sys.exit(1)

    if args.command == 'dbconn':
        output = check_db_connection(conn)
    elif args.command == 'data':
        tables = get_tables(conn)
        for table in tables:
            columns = get_columns(conn, table)

            start_col = next((c for c in EVENT_START_CANDIDATES if c in columns), None)
            end_col = next((c for c in EVENT_END_CANDIDATES if c in columns), None)
            if start_col and end_col:
                output += analyze_event_table(conn, table, start_col, end_col)
                continue

            date_col = next((c for c in DATE_CANDIDATES if c in columns), None)
            if not date_col:
                continue

            station_col = next((c for c in STATION_CANDIDATES if c in columns and c != date_col), None)
            output += analyze_time_series(conn, table, date_col, station_col)

    if conn:
        conn.close()

    print(output)

if __name__ == '__main__':
    main()
