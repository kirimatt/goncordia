-- The fetch query filters state IN ('available', 'scheduled') and orders by
-- (priority DESC, run_at, created_at, id). With state as an index column the
-- planner must merge two ranges and sort every ready job on each fetch. A
-- partial index with the same predicate returns rows already in fetch order,
-- so LIMIT stops after the first rows whatever the backlog size.
CREATE INDEX IF NOT EXISTS idx_goncordia_jobs_fetch_ready
    ON goncordia_jobs (queue, priority DESC, run_at, created_at, id)
    WHERE state IN ('available', 'scheduled');
