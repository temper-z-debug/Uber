USE uber_ncr;

DROP TABLE IF EXISTS stage1_driver_tree_result;

CREATE TABLE stage1_driver_tree_result AS
WITH base AS (
  SELECT
    COUNT(*) AS n_total,
    SUM(booking_status = 'No Driver Found') AS n_no_match,
    SUM(booking_status IN ('Cancelled by Driver','Cancelled by Customer')) AS n_cancel,
    SUM(booking_status = 'Incomplete') AS n_incomplete,
    SUM(booking_status = 'Completed') AS n_completed
  FROM ride_model_gate
),
rates AS (
  SELECT
    n_total,
    n_no_match,
    n_cancel,
    n_incomplete,
    n_completed,
    (n_total - n_no_match) / NULLIF(n_total,0) AS matching_rate,                     -- Matching
    (n_total - n_no_match - n_cancel) / NULLIF((n_total - n_no_match),0) AS fulfillment_rate, -- Fulfillment
    n_completed / NULLIF((n_total - n_no_match - n_cancel),0) AS payment_rate,       -- Payment
    n_completed / NULLIF(n_total,0) AS success_rate
  FROM base
),
calc AS (
  SELECT
    *,
    -- 局部灵敏度（偏导）
    (fulfillment_rate * payment_rate) AS dS_dMatching,
    (matching_rate * payment_rate) AS dS_dFulfillment,
    (matching_rate * fulfillment_rate) AS dS_dPayment,

    -- +1个百分点（绝对）边际拉动
    (fulfillment_rate * payment_rate) * 0.01 AS uplift_matching_1pp,
    (matching_rate * payment_rate) * 0.01 AS uplift_fulfillment_1pp,
    (matching_rate * fulfillment_rate) * 0.01 AS uplift_payment_1pp,

    -- 流失分解（对总流失的贡献）
    (1 - matching_rate) AS loss_no_match,
    (matching_rate * (1 - fulfillment_rate)) AS loss_post_match_cancel,
    (matching_rate * fulfillment_rate * (1 - payment_rate)) AS loss_post_arrival_nonpay
  FROM rates
)
SELECT
  n_total, n_no_match, n_cancel, n_incomplete, n_completed,
  ROUND(matching_rate, 4) AS matching_rate,
  ROUND(fulfillment_rate, 4) AS fulfillment_rate,
  ROUND(payment_rate, 4) AS payment_rate,
  ROUND(success_rate, 4) AS success_rate,

  ROUND(dS_dMatching, 4) AS dS_dMatching,
  ROUND(dS_dFulfillment, 4) AS dS_dFulfillment,
  ROUND(dS_dPayment, 4) AS dS_dPayment,

  ROUND(uplift_matching_1pp, 6) AS uplift_matching_1pp,
  ROUND(uplift_fulfillment_1pp, 6) AS uplift_fulfillment_1pp,
  ROUND(uplift_payment_1pp, 6) AS uplift_payment_1pp,

  ROUND(loss_no_match / NULLIF(1 - success_rate,0), 4) AS loss_share_no_match,
  ROUND(loss_post_match_cancel / NULLIF(1 - success_rate,0), 4) AS loss_share_post_match_cancel,
  ROUND(loss_post_arrival_nonpay / NULLIF(1 - success_rate,0), 4) AS loss_share_post_arrival_nonpay
FROM calc;

SELECT * FROM stage1_driver_tree_result;
