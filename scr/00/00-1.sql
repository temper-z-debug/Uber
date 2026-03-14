USE uber_ncr;

-- ========= 0.0 清理旧产物 =========
DROP VIEW IF EXISTS v_stage0_base;
DROP VIEW IF EXISTS v_customer_pref_payment;

DROP TABLE IF EXISTS dq0_missing_by_status_raw;
DROP TABLE IF EXISTS dq0_missing_cancel_vs_completed;
DROP TABLE IF EXISTS ride_model_gate;
DROP TABLE IF EXISTS dq0_missing_by_status_after;
DROP TABLE IF EXISTS dq0_gate_decision;

-- ========= 0.1 基础清洗视图（只读表达式，不改原表） =========
CREATE VIEW v_stage0_base AS
SELECT
  r.*,
  NULLIF(TRIM(REPLACE(r.payment_method, '\r', '')), 'null') AS payment_method_clean
FROM ride_raw r;

-- ========= 0.2 缺失率交叉审计（Raw） =========
CREATE TABLE dq0_missing_by_status_raw AS
SELECT
  booking_status,
  COUNT(*) AS n,
  SUM(payment_method_clean IS NULL) AS missing_n,
  ROUND(SUM(payment_method_clean IS NULL) / COUNT(*), 4) AS missing_rate
FROM v_stage0_base
GROUP BY booking_status;

-- ========= 0.3 Cancelled vs Completed 的门禁统计（含z-score） =========
CREATE TABLE dq0_missing_cancel_vs_completed AS
WITH s AS (
  SELECT
    SUM(booking_status IN ('Cancelled by Driver','Cancelled by Customer')) AS n_cancel,
    SUM(booking_status IN ('Cancelled by Driver','Cancelled by Customer') AND payment_method_clean IS NULL) AS miss_cancel,
    SUM(booking_status = 'Completed') AS n_completed,
    SUM(booking_status = 'Completed' AND payment_method_clean IS NULL) AS miss_completed
  FROM v_stage0_base
),
r AS (
  SELECT
    n_cancel, miss_cancel, n_completed, miss_completed,
    miss_cancel / n_cancel AS p_cancel,
    miss_completed / n_completed AS p_completed,
    (miss_cancel - miss_completed) / 1.0 AS diff_count,
    (miss_cancel / n_cancel) - (miss_completed / n_completed) AS gap,
    (miss_cancel + miss_completed) / (n_cancel + n_completed) AS p_pool
  FROM s
)
SELECT
  n_cancel, miss_cancel, n_completed, miss_completed,
  ROUND(p_cancel, 4) AS p_cancel,
  ROUND(p_completed, 4) AS p_completed,
  ROUND(gap, 4) AS gap,
  ROUND(
    CASE
      WHEN p_pool*(1-p_pool)*(1/n_cancel + 1/n_completed) = 0 THEN NULL
      ELSE gap / SQRT(p_pool*(1-p_pool)*(1/n_cancel + 1/n_completed))
    END
  , 4) AS z_score,
  CASE
    WHEN gap >= 0.10 AND ABS(
      CASE
        WHEN p_pool*(1-p_pool)*(1/n_cancel + 1/n_completed) = 0 THEN 0
        ELSE gap / SQRT(p_pool*(1-p_pool)*(1/n_cancel + 1/n_completed))
      END
    ) >= 2.58 THEN 'FAIL'
    ELSE 'PASS'
  END AS gate_raw
FROM r;

-- ========= 0.4 按客户历史偏好补齐 =========
CREATE VIEW v_customer_pref_payment AS
WITH pay_cnt AS (
  SELECT customer_id, payment_method_clean AS payment_method, COUNT(*) AS cnt
  FROM v_stage0_base
  WHERE payment_method_clean IS NOT NULL
  GROUP BY customer_id, payment_method_clean
),
ranked AS (
  SELECT
    customer_id,
    payment_method,
    cnt,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cnt DESC, payment_method) AS rn
  FROM pay_cnt
)
SELECT customer_id, payment_method AS preferred_payment
FROM ranked
WHERE rn = 1;

CREATE TABLE ride_model_gate AS
SELECT
  b.*,
  b.payment_method_clean AS payment_method_raw_clean,
  COALESCE(b.payment_method_clean, p.preferred_payment) AS payment_method_filled,
  CASE
    WHEN b.payment_method_clean IS NOT NULL THEN 'observed'
    WHEN p.preferred_payment IS NOT NULL THEN 'imputed_customer_pref'
    ELSE 'missing_unresolved'
  END AS payment_fill_source
FROM v_stage0_base b
LEFT JOIN v_customer_pref_payment p
  ON b.customer_id = p.customer_id;

-- ========= 0.5 补齐后复审 =========
CREATE TABLE dq0_missing_by_status_after AS
SELECT
  booking_status,
  COUNT(*) AS n,
  SUM(payment_fill_source = 'missing_unresolved') AS unresolved_n,
  ROUND(SUM(payment_fill_source = 'missing_unresolved') / COUNT(*), 4) AS unresolved_rate
FROM ride_model_gate
GROUP BY booking_status;

-- ========= 0.6 最终门禁判定 =========
CREATE TABLE dq0_gate_decision AS
WITH a AS (
  SELECT gate_raw, p_cancel, p_completed, gap, z_score
  FROM dq0_missing_cancel_vs_completed
),
b AS (
  SELECT
    SUM(CASE WHEN booking_status IN ('Cancelled by Driver','Cancelled by Customer') THEN unresolved_n ELSE 0 END) AS unresolved_cancel,
    SUM(CASE WHEN booking_status IN ('Cancelled by Driver','Cancelled byCustomer') THEN n ELSE 0 END) AS total_cancel
  FROM dq0_missing_by_status_after
)
SELECT
  NOW() AS audit_time,
  a.gate_raw,
  a.p_cancel,
  a.p_completed,
  a.gap,
  a.z_score,
  ROUND(b.unresolved_cancel / b.total_cancel, 4) AS unresolved_cancel_rate_after_impute,
  CASE
    WHEN a.gate_raw = 'FAIL' OR (b.unresolved_cancel / b.total_cancel) >= 0.20 THEN 'FAIL'
    ELSE 'PASS'
  END AS gate_final,
  CASE
    WHEN a.gate_raw = 'FAIL' OR (b.unresolved_cancel / b.total_cancel) >= 0.20
      THEN 'Stop payment-causal modeling; move to experiment-first strategy.'
    ELSE 'Allowed to enter Stage 2/3 with payment features.'
  END AS action_rule
FROM a CROSS JOIN b;

-- ========= 0.7 查看结果 =========
SELECT * FROM dq0_missing_by_status_raw ORDER BY n DESC;
SELECT * FROM dq0_missing_cancel_vs_completed;
SELECT * FROM dq0_missing_by_status_after ORDER BY n DESC;
SELECT * FROM dq0_gate_decision;

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


