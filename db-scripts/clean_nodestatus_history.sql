-- Keep node status history up to 90 days

DELETE FROM
    public.api2_nodestatushistory
WHERE "timestamp" < now() - interval '90 days';

VACUUM ANALYZE public.api2_nodestatushistory;

DELETE FROM
    public.api2_node
WHERE
  "updated_at" < now() - interval '90 days';

VACUUM ANALYZE public.api2_node;

