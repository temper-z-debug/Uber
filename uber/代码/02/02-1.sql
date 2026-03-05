USE uber_ncr;

DROP VIEW IF EXISTS v_stage2_model_base;
CREATE VIEW v_stage2_model_base AS
SELECT
  booking_id,
  customer_id,
  booking_status,
  CASE WHEN booking_status IN ('Cancelled by Driver','Cancelled by Customer') THEN 1 ELSE 0 END AS is_cancel,
  avg_vtat,
  avg_ctat,
  ride_distance,
  booking_value,
  vehicle_type,
  pickup_location,
  drop_location,
  HOUR(ride_time) AS hour_of_day,
  DAYOFWEEK(ride_date) AS dow
FROM ride_model_gate
WHERE booking_status IN ('Completed','Cancelled by Driver','Cancelled by Customer')
  AND avg_vtat IS NOT NULL;
