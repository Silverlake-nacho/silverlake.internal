# Pinnacle image timeline database load

## Finding

The image timeline can contribute to load on the Pinnacle PostgreSQL database.
Before the optimisation, one page request issued four database queries. Its main
query joined every matching `inventorylog` row to part, vehicle and image data
before discarding duplicate part numbers. In particular, both image lookups were
lateral queries and could be evaluated repeatedly for the same part.

The page now:

1. selects the first relevant log row for each part in a materialised CTE before
   performing the expensive joins and image aggregation; and
2. gets per-user image counts, distinct-part counts and the filter's user list in
   one grouped query rather than three separate queries.

This reduces the page from four PostgreSQL round trips to two and restricts the
expensive portion of the main query to one row per part. It does not prove that
all observed Pinnacle slowness originates in this application; Pinnacle's own
sessions and other clients must be compared during an incident.

## Identifying this application during an incident

Connections set `application_name` to `silverlake-internal` by default. It can be
overridden with `PINNACLE_DB_APPLICATION_NAME`. The two timeline statements also
contain `silverlake-internal:image-timeline` and
`silverlake-internal:image-timeline-stats` comments.

Run the following as a PostgreSQL monitoring user while the slowdown is occurring:

```sql
SELECT pid,
       application_name,
       state,
       now() - query_start AS runtime,
       wait_event_type,
       wait_event,
       LEFT(query, 500) AS query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start;
```

If the long-running or waiting sessions have application name
`silverlake-internal` (or the configured override), this application is involved.
If the load is instead associated with another application name, the timeline is
not the direct source of those sessions. Compare this with CPU, disk latency and
lock monitoring for the same time window rather than drawing a conclusion from
the total database load alone.

## Follow-up database validation

Use `EXPLAIN (ANALYZE, BUFFERS)` on a representative date range during a safe
maintenance window. Confirm that indexes support the filtered log lookup and the
per-part image lookups. Candidate leading columns are:

- `inventorylog (type_id, created, invnumber)`; and
- `image (invnumber, thumbnail, displayorder)`.

Do not add these blindly: first inspect existing Pinnacle indexes and the actual
execution plan, because redundant indexes add cost to Pinnacle's production
writes.
