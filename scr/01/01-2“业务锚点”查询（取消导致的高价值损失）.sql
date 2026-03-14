SELECT
  booking_status,
  COUNT(*) AS n,
  AVG(booking_value) AS avg_value
FROM ride_model_gate
WHERE booking_status IN ('Completed','Cancelled by Driver','Cancelled by Customer')
GROUP BY booking_status;

