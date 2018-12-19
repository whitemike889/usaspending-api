-- No materialized views were harmed in the making of this script.
-- BEGIN;

DROP INDEX "award_category_type_code_idx"; -- replaced with primary key below
DROP INDEX "duns_awardee_idx"; -- replaced with primary key below
DROP INDEX "historic_parent_duns_awardee_idx"; -- removed by request
DROP INDEX "historic_parent_duns_year_idx"; -- removed by request
DROP INDEX "idx_recipient_profile_uniq"; -- re-added as a constraint below
DROP INDEX "legal_entity_recipient_name_idx"; -- removed by request
DROP INDEX "transaction_fabs_afa_generated_unique_key"; -- re-added as a constraint below
DROP INDEX "transaction_fabs_awarding_sub_tier_agency_cc5ccd22_uniq"; -- re-added as a constraint below
DROP INDEX "transaction_fpds_detached_award_proc_unique_key"; -- re-added as constraint below
DROP INDEX "tx_norm_tx_uniq_id"; -- removed by request

-- These were all bigint which is the default.  Probably an artifact of upgrading to PostgreSQL 10.  Previously,
-- the data type could not be specified and was always bigint.  Changed to match the data type of the ID
-- to which they were assigned.
ALTER SEQUENCE "agency_id_seq" AS integer;
ALTER SEQUENCE "appropriation_account_balance_appropriation_account_balance_seq" AS integer;
ALTER SEQUENCE "appropriation_account_balances_quarterly_id_seq" AS integer;
ALTER SEQUENCE "auth_group_id_seq" AS integer;
ALTER SEQUENCE "auth_group_permissions_id_seq" AS integer;
ALTER SEQUENCE "auth_permission_id_seq" AS integer;
ALTER SEQUENCE "auth_user_groups_id_seq" AS integer;
ALTER SEQUENCE "auth_user_id_seq" AS integer;
ALTER SEQUENCE "auth_user_user_permissions_id_seq" AS integer;
ALTER SEQUENCE "awards_subaward_id_seq" AS integer;
ALTER SEQUENCE "budget_authority_id_seq" AS integer;
ALTER SEQUENCE "django_admin_log_id_seq" AS integer;
ALTER SEQUENCE "django_content_type_id_seq" AS integer;
ALTER SEQUENCE "django_migrations_id_seq" AS integer;
ALTER SEQUENCE "download_job_download_job_id_seq" AS integer;
ALTER SEQUENCE "external_data_load_date_external_data_load_date_id_seq" AS integer;
ALTER SEQUENCE "external_data_type_external_data_type_id_seq" AS integer;
ALTER SEQUENCE "federal_account_id_seq" AS integer;
ALTER SEQUENCE "filter_hash_id_seq" AS integer;
ALTER SEQUENCE "financial_accounts_by_awards_financial_accounts_by_awards_i_seq" AS integer;
ALTER SEQUENCE "financial_accounts_by_program_financial_accounts_by_program_seq" AS integer;
ALTER SEQUENCE "frec_map_id_seq" AS integer;
ALTER SEQUENCE "gtas_total_obligation_id_seq" AS integer;
ALTER SEQUENCE "job_status_job_status_id_seq" AS integer;
ALTER SEQUENCE "object_class_id_seq" AS integer;
ALTER SEQUENCE "office_agency_office_agency_id_seq" AS integer;
ALTER SEQUENCE "overall_totals_id_seq" AS integer;
ALTER SEQUENCE "ref_city_county_code_city_county_code_id_seq" AS integer;
ALTER SEQUENCE "ref_program_activity_id_seq" AS integer;
ALTER SEQUENCE "references_cfda_id_seq" AS integer;
ALTER SEQUENCE "references_definition_id_seq" AS integer;
ALTER SEQUENCE "rest_framework_tracking_apirequestlog_id_seq" AS integer;
ALTER SEQUENCE "submission_attributes_submission_id_seq" AS integer;
ALTER SEQUENCE "subtier_agency_subtier_agency_id_seq" AS integer;
ALTER SEQUENCE "tas_program_activity_object_class_quarterly_id_seq" AS integer;
ALTER SEQUENCE "toptier_agency_toptier_agency_id_seq" AS integer;
ALTER SEQUENCE "treasury_appropriation_account_treasury_account_identifier_seq" AS integer;

ALTER TABLE ONLY "appropriation_account_balances"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "appropriation_account_balances_quarterly"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "awards"
    RENAME CONSTRAINT "award_awarding_agency_fk" TO "awards_awarding_agency_id_a8cf5df8_fk_agency_id";
ALTER TABLE ONLY "awards"
    RENAME CONSTRAINT "award_funding_agency_fk" TO "awards_funding_agency_id_aafee1a4_fk_agency_id";
ALTER TABLE ONLY "awards"
    RENAME CONSTRAINT "award_latest_tx_fk" TO "awards_latest_transaction_id_a78dcb0f_fk";
ALTER TABLE ONLY "awards"
    RENAME CONSTRAINT "award_legal_entity_fk" TO "awards_recipient_id_3cdf8905_fk";
ALTER TABLE ONLY "awards"
    RENAME CONSTRAINT "award_ppop_location_fk" TO "awards_place_of_performance_id_7d7369e6_fk";

ALTER TABLE ONLY "awards"
    ALTER COLUMN "generated_unique_award_id" SET NOT NULL,
    ALTER COLUMN "is_fpds" SET NOT NULL,
    ALTER COLUMN "subaward_count" SET NOT NULL,
    ALTER COLUMN "transaction_unique_id" SET NOT NULL,
    ALTER CONSTRAINT "awards_awarding_agency_id_a8cf5df8_fk_agency_id" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "awards_funding_agency_id_aafee1a4_fk_agency_id" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "awards_funding_agency_id_aafee1a4_fk_agency_id" DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "award_category"
    ADD PRIMARY KEY ("type_code");

ALTER TABLE ONLY "duns"
    ALTER COLUMN "awardee_or_recipient_uniqu" SET NOT NULL,
    ALTER COLUMN "broker_duns_id" SET NOT NULL,
    ALTER COLUMN "update_date" SET NOT NULL,
    ADD CONSTRAINT "duns_pkey" PRIMARY KEY ("awardee_or_recipient_uniqu");

ALTER TABLE ONLY "recipient_lookup"
    ADD CONSTRAINT "recipient_lookup_duns_ae948c75_uniq" UNIQUE ("duns");

ALTER TABLE ONLY "financial_accounts_by_awards"
    RENAME CONSTRAINT "faba_award_fk" TO "financial_accounts_by_awards_award_id_eb90a5fa_fk_awards_id";

ALTER TABLE ONLY "financial_accounts_by_awards"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "financial_accounts_by_program_activity_object_class"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "historic_parent_duns"
    ALTER COLUMN "awardee_or_recipient_uniqu" SET NOT NULL,
    ALTER COLUMN "broker_historic_duns_id" SET NOT NULL,
    ALTER COLUMN "broker_historic_duns_id" TYPE int USING "broker_historic_duns_id"::int, -- from text
    ADD CONSTRAINT "historic_parent_duns_broker_historic_duns_id_0dd7a627_pk" PRIMARY KEY ("broker_historic_duns_id");

UPDATE "legal_entity" SET "business_categories" = '{}' WHERE "business_categories" IS NULL;

ALTER TABLE ONLY "legal_entity"
    RENAME CONSTRAINT "le_location_fk" TO "legal_entity_location_id_7f712296_fk";

ALTER TABLE ONLY "legal_entity"
    ALTER COLUMN "is_fpds" SET NOT NULL,
    ALTER COLUMN "reporting_period_end" TYPE date USING "reporting_period_end"::date, -- from text
    ALTER COLUMN "reporting_period_start" TYPE date USING "reporting_period_start"::date, -- from text
    ALTER COLUMN "transaction_unique_id" SET NOT NULL,
    ALTER COLUMN "business_categories" SET NOT NULL;

ALTER TABLE ONLY "recipient_profile"
    ALTER COLUMN "last_12_months" SET NOT NULL,
    ALTER COLUMN "recipient_affiliations" SET NOT NULL,
    ALTER COLUMN "recipient_level" TYPE character varying(1), -- from fixed width char(1)
    DROP CONSTRAINT "recipient_profile_id_key",
    ADD CONSTRAINT "recipient_profile_pkey" PRIMARY KEY (id),
    ADD CONSTRAINT "recipient_profile_recipient_hash_recipient_level_ee7ecd55_uniq" UNIQUE ("recipient_hash", "recipient_level");

ALTER TABLE ONLY "references_cfda"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "references_legalentityofficers"
    ADD CONSTRAINT "references_legalenti_legal_entity_id_2e4739e1_fk_legal_ent" FOREIGN KEY ("legal_entity_id") REFERENCES "legal_entity"("legal_entity_id") DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "references_location"
    ALTER COLUMN "is_fpds" SET NOT NULL,
    ALTER COLUMN "place_of_performance_flag" SET NOT NULL,
    ALTER COLUMN "recipient_flag" SET NOT NULL,
    ALTER COLUMN "reporting_period_end" TYPE date USING "reporting_period_end"::date, -- from text
    ALTER COLUMN "reporting_period_start" TYPE date USING "reporting_period_start"::date, -- from text
    ALTER COLUMN "transaction_unique_id" SET NOT NULL;

ALTER TABLE ONLY "subaward"
    RENAME CONSTRAINT "subaward_prime_recipient_id_dcb32e13_fk_legal_ent" TO "subaward_prime_recipient_id_dcb32e13_fk";
ALTER TABLE ONLY "subaward"
    RENAME CONSTRAINT "awards_subaward_award_id_82b3b128_fk_awards_id" TO "subaward_award_id_9e8ee0ce_fk";

ALTER TABLE ONLY "subaward"
    ALTER COLUMN "award_id" TYPE bigint, -- from int
    ALTER COLUMN "prime_recipient_id" TYPE bigint; -- from int

ALTER TABLE ONLY "tas_program_activity_object_class_quarterly"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER TABLE ONLY "transaction_fabs"
    ALTER COLUMN "afa_generated_unique" SET NOT NULL,
    ALTER COLUMN "is_active" SET NOT NULL,
    ALTER COLUMN "submission_id" TYPE integer, -- from numeric
    ADD CONSTRAINT "transaction_fabs_afa_generated_unique_c943a0be_uniq" UNIQUE ("afa_generated_unique"),
    ADD CONSTRAINT "transaction_fabs_awarding_sub_tier_agency_cc5ccd22_uniq" UNIQUE ("awarding_sub_tier_agency_c", "award_modification_amendme", "fain", "uri");

ALTER TABLE ONLY "transaction_fabs"
    RENAME CONSTRAINT "tx_fabs_tx_norm_fk" TO "transaction_fabs_transaction_id_d38eed48_fk_transacti";

ALTER TABLE ONLY "transaction_fpds"
    ADD CONSTRAINT "transaction_fpds_detached_award_proc_unique_key" UNIQUE ("detached_award_proc_unique");

ALTER TABLE ONLY "transaction_fpds"
    RENAME CONSTRAINT "tx_fpds_tx_norm_fk" TO "transaction_fpds_transaction_id_4349b52d_fk_transacti";

ALTER TABLE ONLY "transaction_normalized"
    ALTER COLUMN "action_date" SET NOT NULL,
    ALTER COLUMN "award_id" SET NOT NULL,
    ALTER COLUMN "drv_award_transaction_usaspend" TYPE numeric(23, 2) USING "drv_award_transaction_usaspend"::numeric, -- from text
    ALTER COLUMN "drv_current_total_award_value_amount_adjustment" TYPE numeric(23, 2) USING "drv_current_total_award_value_amount_adjustment"::numeric, -- from text
    ALTER COLUMN "drv_potential_total_award_value_amount_adjustment" TYPE numeric(23, 2) USING "drv_potential_total_award_value_amount_adjustment"::numeric, -- from text
    ALTER COLUMN "generated_unique_award_id" SET NOT NULL,
    ALTER COLUMN "is_fpds" SET NOT NULL,
    ALTER COLUMN "transaction_unique_id" SET NOT NULL,
    ALTER CONSTRAINT "tx_norm_awarding_agency_fk" DEFERRABLE INITIALLY DEFERRED,
    ALTER CONSTRAINT "tx_norm_funding_agency_fk" DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "transaction_normalized"
    RENAME CONSTRAINT "tx_norm_award_fk" TO "transaction_normalized_award_id_aab95fbf_fk";
ALTER TABLE ONLY "transaction_normalized"
    RENAME CONSTRAINT "tx_norm_awarding_agency_fk" TO "transaction_normalized_awarding_agency_id_27b1cd91_fk_agency_id";
ALTER TABLE ONLY "transaction_normalized"
    RENAME CONSTRAINT "tx_norm_funding_agency_fk" TO "transaction_normalized_funding_agency_id_27ab4331_fk_agency_id";
ALTER TABLE ONLY "transaction_normalized"
    RENAME CONSTRAINT "tx_norm_legal_entity_fk" TO "transaction_normalized_recipient_id_f8f784f2_fk";
ALTER TABLE ONLY "transaction_normalized"
    RENAME CONSTRAINT "tx_norm_ppop_location_fk" TO "transaction_normalized_place_of_performance_id_ab44d845_fk";

ALTER TABLE ONLY "treasury_appropriation_account"
    ALTER COLUMN "data_source" TYPE text; -- from varchar

ALTER INDEX "idx_recipient_profile_unique_id" RENAME TO "recipient_p_recipie_7039a5_idx";
ALTER INDEX "transaction_fabs_afa_generated_unique_bb7b8f4b_like" RENAME TO "transaction_fabs_afa_generated_unique_c943a0be_like";
ALTER INDEX "tx_fabs_updated_at_asc" RENAME TO "transaction_fabs_updated_at_1091bb24";
ALTER INDEX "tx_fpds_updated_at_asc" RENAME TO "transaction_fpds_updated_at_e69f8257";

CREATE INDEX "legal_entity_recipient_name_idx" ON "legal_entity" USING btree ("recipient_name");

-- Move away from open ended NUMERICs in favor of well defined.  Also, standardize on (23, 2).
ALTER TABLE "appropriation_account_balances"
    ALTER COLUMN "adjustments_to_unobligated_balance_brought_forward_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "borrowing_authority_amount_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "budget_authority_appropriated_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "budget_authority_unobligated_balance_brought_forward_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "contract_authority_amount_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "deobligations_recoveries_refunds_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "drv_obligations_unpaid_amount" TYPE numeric(23, 2),
    ALTER COLUMN "drv_other_obligated_amount" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_incurred_total_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "other_budgetary_resources_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "spending_authority_from_offsetting_collections_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "status_of_budgetary_resources_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "total_budgetary_resources_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "unobligated_balance_cpe" TYPE numeric(23, 2);

ALTER TABLE "appropriation_account_balances_quarterly"
    ALTER COLUMN "adjustments_to_unobligated_balance_brought_forward_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "borrowing_authority_amount_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "budget_authority_appropriated_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "budget_authority_unobligated_balance_brought_forward_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "contract_authority_amount_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "deobligations_recoveries_refunds_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_incurred_total_by_tas_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "other_budgetary_resources_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "spending_authority_from_offsetting_collections_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "status_of_budgetary_resources_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "total_budgetary_resources_amount_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "unobligated_balance_cpe" TYPE numeric(23, 2);

ALTER TABLE "financial_accounts_by_awards"
    ALTER COLUMN "deobligations_recoveries_refunds_of_prior_year_by_award_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "drv_obligations_incurred_total_by_award" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_award_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_award_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_incurred_total_by_award_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "transaction_obligated_amount" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490200_delivered_orders_obligations_paid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe" TYPE numeric(23, 2);

ALTER TABLE "financial_accounts_by_program_activity_object_class"
    ALTER COLUMN "deobligations_recoveries_refund_pri_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "drv_obligations_incurred_by_program_object_class" TYPE numeric(23, 2),
    ALTER COLUMN "drv_obligations_undelivered_orders_unpaid" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_program_object_class_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_incurred_by_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490200_delivered_orders_obligations_paid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe" TYPE numeric(23, 2);

ALTER TABLE "gtas_total_obligation"
    ALTER COLUMN total_obligation TYPE numeric(23, 2);

ALTER TABLE "overall_totals"
    ALTER COLUMN "total_budget_authority" TYPE numeric(23, 2);

ALTER TABLE "recipient_profile"
    ALTER COLUMN "last_12_contracts" TYPE numeric(23, 2),
    ALTER COLUMN "last_12_direct_payments" TYPE numeric(23, 2),
    ALTER COLUMN "last_12_grants" TYPE numeric(23, 2),
    ALTER COLUMN "last_12_loans" TYPE numeric(23, 2),
    ALTER COLUMN "last_12_months" TYPE numeric(23, 2),
    ALTER COLUMN "last_12_other" TYPE numeric(23, 2);

ALTER TABLE "references_legalentityofficers"
    ALTER COLUMN "officer_1_amount" TYPE numeric(23, 2),
    ALTER COLUMN "officer_2_amount" TYPE numeric(23, 2),
    ALTER COLUMN "officer_3_amount" TYPE numeric(23, 2),
    ALTER COLUMN "officer_4_amount" TYPE numeric(23, 2),
    ALTER COLUMN "officer_5_amount" TYPE numeric(23, 2);

ALTER TABLE "state_data"
    ALTER COLUMN "median_household_income" TYPE numeric(23, 2);

ALTER TABLE "subaward"
    ALTER COLUMN "amount" TYPE numeric(23, 2);

ALTER TABLE "tas_program_activity_object_class_quarterly"
    ALTER COLUMN "deobligations_recoveries_refund_pri_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlay_amount_by_program_object_class_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_delivered_orders_paid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "gross_outlays_undelivered_orders_prepaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_delivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_incurred_by_program_object_class_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "obligations_undelivered_orders_unpaid_total_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480100_undelivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490100_delivered_orders_obligations_unpaid_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490200_delivered_orders_obligations_paid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl490800_authority_outlayed_not_yet_disbursed_fyb" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe" TYPE numeric(23, 2),
    ALTER COLUMN "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe" TYPE numeric(23, 2);

ALTER TABLE "transaction_fpds"
    ALTER COLUMN "federal_action_obligation" TYPE numeric(23, 2);

-- Standardize on timestamps with time zone.
ALTER TABLE "transaction_fabs"
    ALTER COLUMN "created_at" TYPE timestamp with time zone,
    ALTER COLUMN "modified_at" TYPE timestamp with time zone,
    ALTER COLUMN "updated_at" TYPE timestamp with time zone;

ALTER TABLE "transaction_fpds"
    ALTER COLUMN "created_at" TYPE timestamp with time zone,
    ALTER COLUMN "updated_at" TYPE timestamp with time zone;

-- COMMIT;
