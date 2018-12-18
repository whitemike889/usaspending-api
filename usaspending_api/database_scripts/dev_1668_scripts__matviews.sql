-- The following materialized views will need to be dropped before running these:
--    drop materialized view summary_award_view;
--    drop materialized view summary_state_view;
--    drop materialized view summary_transaction_fed_acct_view;
--    drop materialized view summary_transaction_geo_view;
--    drop materialized view summary_transaction_month_view;
--    drop materialized view summary_transaction_recipient_view;
--    drop materialized view summary_transaction_view;
--    drop materialized view summary_view;
--    drop materialized view summary_view_cfda_number;
--    drop materialized view summary_view_naics_codes;
--    drop materialized view summary_view_psc_codes;
--    drop materialized view universal_award_matview;
--    drop materialized view universal_transaction_matview;
BEGIN;

ALTER TABLE "awards"
    ALTER COLUMN "base_and_all_options_value" TYPE numeric(23, 2),
    ALTER COLUMN "non_federal_funding_amount" TYPE numeric(23, 2),
    ALTER COLUMN "potential_total_value_of_award" TYPE numeric(23, 2),
    ALTER COLUMN "total_funding_amount" TYPE numeric(23, 2),
    ALTER COLUMN "total_loan_value" TYPE numeric(23, 2),
    ALTER COLUMN "total_obligation" TYPE numeric(23, 2),
    ALTER COLUMN "total_outlay" TYPE numeric(23, 2),
    ALTER COLUMN "total_subaward_amount" TYPE numeric(23, 2),
    ALTER COLUMN "total_subsidy_cost" TYPE numeric(23, 2);

ALTER TABLE "transaction_fabs"
    ALTER COLUMN "face_value_loan_guarantee" TYPE numeric(23, 2),
    ALTER COLUMN "federal_action_obligation" TYPE numeric(23, 2),
    ALTER COLUMN "non_federal_funding_amount" TYPE numeric(23, 2),
    ALTER COLUMN "original_loan_subsidy_cost" TYPE numeric(23, 2);

ALTER TABLE "transaction_normalized"
    ALTER COLUMN "federal_action_obligation" TYPE numeric(23, 2),
    ALTER COLUMN "funding_amount" TYPE numeric(23, 2),
    ALTER COLUMN "non_federal_funding_amount" TYPE numeric(23, 2),
    ALTER COLUMN "original_loan_subsidy_cost" TYPE numeric(23, 2);

COMMIT;
