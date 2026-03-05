USE uber_ncr;

DROP VIEW IF EXISTS v_stage3_psm_base;
CREATE VIEW v_stage3_psm_base AS
WITH freq AS (
  SELECT customer_id, COUNT(*) AS trip_cnt
  FROM ride_model_gate
  GROUP BY customer_id
),
seg AS (
  SELECT
    customer_id,
    CASE WHEN NTILE(4) OVER (ORDER BY trip_cnt DESC)=1 THEN 1 ELSE 0 END AS high_freq
  FROM freq
)
SELECT
  m.booking_id,
  m.customer_id,
  m.is_cancel,
  m.avg_vtat,
  m.hour_of_day,
  m.dow,
  m.vehicle_type,
  m.pickup_location,
  m.ride_distance,
  COALESCE(s.high_freq,0) AS high_freq,
  CASE WHEN m.avg_vtat >= 6 AND m.avg_vtat < 10 THEN 1 ELSE 0 END AS treat_vtat_6_10
FROM v_stage2_model_base m
LEFT JOIN seg s
  ON m.customer_id = s.customer_id
WHERE m.avg_vtat IS NOT NULL;
