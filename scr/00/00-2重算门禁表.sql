USE uber_ncr;

DROP TABLE IF EXISTS dq0_gate_decision;

CREATE TABLE dq0_gate_decision AS
WITH a AS (
  SELECT gate_raw, p_cancel, p_completed, gap, z_score
  FROM dq0_missing_cancel_vs_completed
),
b AS (
  SELECT
    SUM(CASE WHEN booking_status IN ('Cancelled by Driver','Cancelled by Customer')
             THEN unresolved_n ELSE 0 END) AS unresolved_cancel,
    SUM(CASE WHEN booking_status IN ('Cancelled by Driver','Cancelled by Customer')
             THEN n ELSE 0 END) AS total_cancel
  FROM dq0_missing_by_status_after
)
SELECT
  NOW() AS audit_time,
  a.gate_raw,
  a.p_cancel,
  a.p_completed,
  a.gap,
  a.z_score,
  ROUND(b.unresolved_cancel / NULLIF(b.total_cancel,0), 4) AS unresolved_cancel_rate_after_impute,
  CASE
    WHEN a.gate_raw='FAIL'
      OR (b.unresolved_cancel / NULLIF(b.total_cancel,0)) >= 0.20
    THEN 'FAIL' ELSE 'PASS'
  END AS gate_final,
  CASE
    WHEN a.gate_raw='FAIL'
      OR (b.unresolved_cancel / NULLIF(b.total_cancel,0)) >= 0.20
    THEN 'Stop payment-causal modeling; move to experiment-first strategy.'
    ELSE 'Allowed to enter Stage 2/3 with payment features.'
  END AS action_rule
FROM a CROSS JOIN b;

SELECT * FROM dq0_gate_decision;
