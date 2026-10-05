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

## Connection privacy and monitoring

The application deliberately does not set a custom PostgreSQL `application_name`
and does not embed identifying comments in its SQL. The database therefore gets
ordinary parameterised SQL without an application-specific label. This avoids
adding Silverlake-specific markers to the activity Pinnacle support sees, while
retaining the query improvements above.

This is not intended to conceal database activity or impersonate the Pinnacle
application. PostgreSQL administrators can always inspect the SQL issued to their
server, and the application must continue to use its authorised database account.
Performance attribution should instead use application-side request timings and
database-wide CPU, disk and lock measurements for the same time window.

For stronger isolation, the preferred longer-term solution is a read-only
reporting replica or reporting API supplied by Pinnacle. The image timeline can
then run analytical reads without competing with the primary Pinnacle workload.
Until that is available, keep the two-query implementation, use bounded date
ranges, and avoid adding query tags solely for monitoring.

## Follow-up database validation

Use `EXPLAIN (ANALYZE, BUFFERS)` on a representative date range during a safe
maintenance window. Confirm that indexes support the filtered log lookup and the
per-part image lookups. Candidate leading columns are:

- `inventorylog (type_id, created, invnumber)`; and
- `image (invnumber, thumbnail, displayorder)`.

Do not add these blindly: first inspect existing Pinnacle indexes and the actual
execution plan, because redundant indexes add cost to Pinnacle's production
writes.
