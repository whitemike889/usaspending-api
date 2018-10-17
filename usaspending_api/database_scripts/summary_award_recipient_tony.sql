CREATE MATERIALIZED VIEW summary_award_recipient_tony AS (
  SELECT
    awards.id AS award_id,
    earliest_transaction.min_date::date AS action_date,
    COALESCE(recipient_lookup.recipient_hash, MD5(
    UPPER(COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)))::uuid
  )::uuid AS recipient_hash,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id
  FROM awards as base_award
  LEFT JOIN LATERAL (
    SELECT
        award_id,
        MIN(action_date::date) AS min_date
    FROM transaction_normalized
    GROUP BY award_id
   ) earliest_transaction ON base_award.id = earliest_transaction.award_id
  INNER JOIN awards ON earliest_transaction.award_id = awards.id
  INNER JOIN transaction_normalized ON awards.latest_transaction_id = transaction_normalized.id
  LEFT OUTER JOIN transaction_fabs ON transaction_normalized.id = transaction_fabs.transaction_id
  LEFT OUTER JOIN transaction_fpds ON transaction_normalized.id = transaction_fpds.transaction_id
  LEFT OUTER JOIN
    (SELECT
      recipient_hash,
      legal_business_name AS recipient_name,
      duns
    FROM recipient_lookup AS rlv
    ) recipient_lookup ON (
      recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AND
      COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
  )
);

CREATE INDEX idx_temp_rec_awrds_0 ON summary_award_recipient_tony USING BTREE(recipient_hash);
CREATE INDEX idx_temp_rec_awrds_1 ON summary_award_recipient_tony USING BTREE(award_id);
CREATE INDEX idx_temp_rec_awrds_2 ON summary_award_recipient_tony USING BTREE(parent_recipient_unique_id);