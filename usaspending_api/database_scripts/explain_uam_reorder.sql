DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_old CASCADE;

CREATE MATERIALIZED VIEW universal_award_matview_temp AS
SELECT
  to_tsvector(CONCAT_WS(' ',
    COALESCE(recipient_lookup.recipient_name, legal_entity.recipient_name),
    contract_data.naics,
    contract_data.naics_description,
    psc.description,
    (SELECT string_agg(tn.description, ' ') FROM transaction_normalized AS tn WHERE tn.award_id = awards.id GROUP BY tn.award_id)
  )) AS keyword_ts_vector,
  to_tsvector(CONCAT_WS(' ', awards.piid, awards.fain, awards.uri)) AS award_ts_vector,
  to_tsvector(COALESCE(recipient_lookup.recipient_name, legal_entity.recipient_name)) AS recipient_name_ts_vector,

  awards.id AS award_id,
  awards.category,
  awards.type,
  awards.type_description,
  awards.piid,
  awards.fain,
  awards.uri,
  awards.total_obligation,
  awards.description,
  obligation_to_enum(awards.total_obligation) AS total_obl_bin,
  awards.total_subsidy_cost,
  awards.total_loan_value,

  awards.recipient_id,
  UPPER(COALESCE(recipient_lookup.recipient_name, legal_entity.recipient_name)) AS recipient_name,
  legal_entity.recipient_unique_id AS recipient_unique_id,
  legal_entity.parent_recipient_unique_id,
  legal_entity.business_categories,

  latest_transaction.action_date,
  latest_transaction.fiscal_year,
  latest_transaction.last_modified_date,
  awards.period_of_performance_start_date,
  awards.period_of_performance_current_end_date,
  awards.date_signed,

  transaction_fabs.original_loan_subsidy_cost,
  transaction_fabs.face_value_loan_guarantee,

  latest_transaction.awarding_agency_id,
  latest_transaction.funding_agency_id,
  TAA.name AS awarding_toptier_agency_name,
  TFA.name AS funding_toptier_agency_name,
  SAA.name AS awarding_subtier_agency_name,
  SFA.name AS funding_subtier_agency_name,
  TAA.cgac_code AS awarding_toptier_agency_code,
  TFA.cgac_code AS funding_toptier_agency_code,
  SAA.subtier_code AS awarding_subtier_agency_code,
  SFA.subtier_code AS funding_subtier_agency_code,

  COALESCE(contract_data.legal_entity_country_code, transaction_fabs.legal_entity_country_code) AS recipient_location_country_code,
  COALESCE(contract_data.legal_entity_country_name, transaction_fabs.legal_entity_country_name) AS recipient_location_country_name,
  COALESCE(contract_data.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  COALESCE(contract_data.legal_entity_county_code, transaction_fabs.legal_entity_county_code) AS recipient_location_county_code,
  COALESCE(contract_data.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  COALESCE(contract_data.legal_entity_congressional, transaction_fabs.legal_entity_congressional) AS recipient_location_congressional_code,
  COALESCE(contract_data.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5,

  place_of_performance.country_name AS pop_country_name,
  place_of_performance.location_country_code AS pop_country_code,
  place_of_performance.state_code AS pop_state_code,
  place_of_performance.county_code AS pop_county_code,
  place_of_performance.county_name AS pop_county_name,
  place_of_performance.city_code AS pop_city_code,
  place_of_performance.zip5 AS pop_zip5,
  place_of_performance.congressional_code AS pop_congressional_code,

  transaction_fabs.cfda_number,
  transaction_fabs.sai_number,
  contract_data.pulled_from,
  contract_data.type_of_contract_pricing,
  contract_data.extent_competed,
  contract_data.type_set_aside,

  contract_data.product_or_service_code,
  psc.description AS product_or_service_description,
  contract_data.naics AS naics_code,
  contract_data.naics_description
FROM
  awards
INNER JOIN
  transaction_normalized AS latest_transaction
    ON (awards.latest_transaction_id = latest_transaction.id)
LEFT OUTER JOIN
  transaction_fabs
    ON (awards.latest_transaction_id = transaction_fabs.transaction_id)
LEFT OUTER JOIN
  transaction_fpds AS contract_data
    ON (awards.latest_transaction_id = contract_data.transaction_id)
INNER JOIN
  legal_entity
    ON (awards.recipient_id = legal_entity.legal_entity_id)
LEFT OUTER JOIN
  (SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.duns = legal_entity.recipient_unique_id AND legal_entity.recipient_unique_id IS NOT NULL
LEFT OUTER JOIN
  references_location AS place_of_performance ON (awards.place_of_performance_id = place_of_performance.location_id)
LEFT OUTER JOIN
  psc ON (contract_data.product_or_service_code = psc.code)
LEFT OUTER JOIN
  agency AS AA
    ON (awards.awarding_agency_id = AA.id)
LEFT OUTER JOIN
  toptier_agency AS TAA
    ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SAA
    ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT OUTER JOIN
  agency AS FA ON (awards.funding_agency_id = FA.id)
LEFT OUTER JOIN
  toptier_agency AS TFA
    ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SFA
    ON (FA.subtier_agency_id = SFA.subtier_agency_id)
WHERE
  (awards.category IS NOT NULL OR contract_data.pulled_from = 'IDV') AND
  latest_transaction.action_date >= '2000-10-01'
ORDER BY
  awards.total_obligation DESC NULLS LAST
WITH NO DATA;

CREATE UNIQUE INDEX idx_a9f78616$932_id_temp ON universal_award_matview_temp USING BTREE(award_id) WITH (fillfactor = 97);
CREATE INDEX idx_a9f78616$932_category_temp ON universal_award_matview_temp USING BTREE(category) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_type_temp ON universal_award_matview_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_type_temp ON universal_award_matview_temp USING BTREE(type DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_type_desc_temp ON universal_award_matview_temp USING BTREE(type_description DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_fain_temp ON universal_award_matview_temp USING BTREE(UPPER(fain) DESC NULLS LAST) WITH (fillfactor = 97) WHERE fain IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_piid_temp ON universal_award_matview_temp USING BTREE(UPPER(piid) DESC NULLS LAST) WITH (fillfactor = 97) WHERE piid IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_total_obligation_temp ON universal_award_matview_temp USING BTREE(total_obligation) WITH (fillfactor = 97) WHERE total_obligation IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_total_obligation_temp ON universal_award_matview_temp USING BTREE(total_obligation DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_total_obl_bin_temp ON universal_award_matview_temp USING BTREE(total_obl_bin) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
DO $$ BEGIN RAISE NOTICE '10 indexes created, 61 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE(total_subsidy_cost) WITH (fillfactor = 97) WHERE total_subsidy_cost IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_total_loan_value_temp ON universal_award_matview_temp USING BTREE(total_loan_value) WITH (fillfactor = 97) WHERE total_loan_value IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE(total_subsidy_cost DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_total_loan_value_temp ON universal_award_matview_temp USING BTREE(total_loan_value DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_period_of_performance_start_date_temp ON universal_award_matview_temp USING BTREE(period_of_performance_start_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_period_of_performance_current_end_date_temp ON universal_award_matview_temp USING BTREE(period_of_performance_current_end_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_name_temp ON universal_award_matview_temp USING BTREE(recipient_name) WITH (fillfactor = 97) WHERE recipient_name IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_parent_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97) WHERE parent_recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_action_date_temp ON universal_award_matview_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
DO $$ BEGIN RAISE NOTICE '20 indexes created, 51 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_last_modified_date_temp ON universal_award_matview_temp USING BTREE(last_modified_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_a9f78616$932_awarding_agency_id_temp ON universal_award_matview_temp USING BTREE(awarding_agency_id ASC NULLS LAST) WITH (fillfactor = 97) WHERE awarding_agency_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_funding_agency_id_temp ON universal_award_matview_temp USING BTREE(funding_agency_id ASC NULLS LAST) WITH (fillfactor = 97) WHERE funding_agency_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE(awarding_toptier_agency_name DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_ordered_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE(awarding_subtier_agency_name DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE(awarding_toptier_agency_name) WITH (fillfactor = 97) WHERE awarding_toptier_agency_name IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE(awarding_subtier_agency_name) WITH (fillfactor = 97) WHERE awarding_subtier_agency_name IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_funding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE(funding_toptier_agency_name) WITH (fillfactor = 97) WHERE funding_toptier_agency_name IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_funding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE(funding_subtier_agency_name) WITH (fillfactor = 97) WHERE funding_subtier_agency_name IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_country_code_temp ON universal_award_matview_temp USING BTREE(recipient_location_country_code) WITH (fillfactor = 97) WHERE recipient_location_country_code IS NOT NULL AND action_date >= '2007-10-01';
DO $$ BEGIN RAISE NOTICE '30 indexes created, 41 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_recipient_location_state_code_temp ON universal_award_matview_temp USING BTREE(recipient_location_state_code) WITH (fillfactor = 97) WHERE recipient_location_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_county_code_temp ON universal_award_matview_temp USING BTREE(recipient_location_county_code) WITH (fillfactor = 97) WHERE recipient_location_county_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_zip5_temp ON universal_award_matview_temp USING BTREE(recipient_location_zip5) WITH (fillfactor = 97) WHERE recipient_location_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_cong_code_temp ON universal_award_matview_temp USING BTREE(recipient_location_congressional_code) WITH (fillfactor = 97) WHERE recipient_location_congressional_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pop_country_code_temp ON universal_award_matview_temp USING BTREE(pop_country_code) WITH (fillfactor = 97) WHERE pop_country_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pop_state_code_temp ON universal_award_matview_temp USING BTREE(pop_state_code) WITH (fillfactor = 97) WHERE pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pop_county_code_temp ON universal_award_matview_temp USING BTREE(pop_county_code) WITH (fillfactor = 97) WHERE pop_county_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pop_zip5_temp ON universal_award_matview_temp USING BTREE(pop_zip5) WITH (fillfactor = 97) WHERE pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pop_congressional_code_temp ON universal_award_matview_temp USING BTREE(pop_congressional_code) WITH (fillfactor = 97) WHERE pop_congressional_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_simple_pop_geolocation_temp ON universal_award_matview_temp USING BTREE(pop_state_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
DO $$ BEGIN RAISE NOTICE '40 indexes created, 31 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_compound_geo_pop_1_temp ON universal_award_matview_temp USING BTREE(pop_state_code, pop_county_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_geo_pop_2_temp ON universal_award_matview_temp USING BTREE(pop_state_code, pop_congressional_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_geo_pop_3_temp ON universal_award_matview_temp USING BTREE(pop_zip5, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_simple_recipient_location_geolocation_temp ON universal_award_matview_temp USING BTREE(recipient_location_state_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND recipient_location_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_geo_rl_1_temp ON universal_award_matview_temp USING BTREE(recipient_location_state_code, recipient_location_county_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND recipient_location_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_geo_rl_2_temp ON universal_award_matview_temp USING BTREE(recipient_location_state_code, recipient_location_congressional_code, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND recipient_location_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_geo_rl_3_temp ON universal_award_matview_temp USING BTREE(recipient_location_zip5, action_date) WITH (fillfactor = 97) WHERE recipient_location_country_code = 'USA' AND recipient_location_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_cfda_number_temp ON universal_award_matview_temp USING BTREE(cfda_number) WITH (fillfactor = 97) WHERE cfda_number IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_pulled_from_temp ON universal_award_matview_temp USING BTREE(pulled_from) WITH (fillfactor = 97) WHERE pulled_from IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_type_of_contract_pricing_temp ON universal_award_matview_temp USING BTREE(type_of_contract_pricing) WITH (fillfactor = 97) WHERE type_of_contract_pricing IS NOT NULL AND action_date >= '2007-10-01';
DO $$ BEGIN RAISE NOTICE '50 indexes created, 21 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_extent_competed_temp ON universal_award_matview_temp USING BTREE(extent_competed) WITH (fillfactor = 97) WHERE extent_competed IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_type_set_aside_temp ON universal_award_matview_temp USING BTREE(type_set_aside) WITH (fillfactor = 97) WHERE type_set_aside IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_product_or_service_code_temp ON universal_award_matview_temp USING BTREE(product_or_service_code) WITH (fillfactor = 97) WHERE product_or_service_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_gin_product_or_service_description_temp ON universal_award_matview_temp USING GIN((product_or_service_description) gin_trgm_ops);
CREATE INDEX idx_a9f78616$932_naics_temp ON universal_award_matview_temp USING BTREE(naics_code) WITH (fillfactor = 97) WHERE naics_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_gin_naics_code_temp ON universal_award_matview_temp USING GIN(naics_code gin_trgm_ops);
CREATE INDEX idx_a9f78616$932_gin_naics_description_temp ON universal_award_matview_temp USING GIN(UPPER(naics_description) gin_trgm_ops);
CREATE INDEX idx_a9f78616$932_gin_business_categories_temp ON universal_award_matview_temp USING GIN(business_categories);
CREATE INDEX idx_a9f78616$932_keyword_ts_vector_temp ON universal_award_matview_temp USING GIN(keyword_ts_vector);
CREATE INDEX idx_a9f78616$932_award_ts_vector_temp ON universal_award_matview_temp USING GIN(award_ts_vector);
DO $$ BEGIN RAISE NOTICE '60 indexes created, 11 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_recipient_name_ts_vector_temp ON universal_award_matview_temp USING GIN(recipient_name_ts_vector);
CREATE INDEX idx_a9f78616$932_compound_psc_action_date_temp ON universal_award_matview_temp USING BTREE(product_or_service_code, action_date) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_naics_action_date_temp ON universal_award_matview_temp USING BTREE(naics_code, action_date) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_compound_cfda_action_date_temp ON universal_award_matview_temp USING BTREE(cfda_number, action_date) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_a9f78616$932_awarding_toptier_agency_name_pre2008_temp ON universal_award_matview_temp USING BTREE(awarding_toptier_agency_name) WITH (fillfactor = 97) WHERE awarding_toptier_agency_name IS NOT NULL AND action_date < '2007-10-01';
CREATE INDEX idx_a9f78616$932_awarding_subtier_agency_name_pre2008_temp ON universal_award_matview_temp USING BTREE(awarding_subtier_agency_name) WITH (fillfactor = 97) WHERE awarding_subtier_agency_name IS NOT NULL AND action_date < '2007-10-01';
CREATE INDEX idx_a9f78616$932_type_pre2008_temp ON universal_award_matview_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date < '2007-10-01';
CREATE INDEX idx_a9f78616$932_pulled_from_pre2008_temp ON universal_award_matview_temp USING BTREE(pulled_from) WITH (fillfactor = 97) WHERE pulled_from IS NOT NULL AND action_date < '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_country_code_pre2008_temp ON universal_award_matview_temp USING BTREE(recipient_location_country_code) WITH (fillfactor = 97) WHERE recipient_location_country_code IS NOT NULL AND action_date < '2007-10-01';
CREATE INDEX idx_a9f78616$932_recipient_location_state_code_pre2008_temp ON universal_award_matview_temp USING BTREE(recipient_location_state_code) WITH (fillfactor = 97) WHERE recipient_location_state_code IS NOT NULL AND action_date < '2007-10-01';
DO $$ BEGIN RAISE NOTICE '70 indexes created, 1 remaining'; END $$;
CREATE INDEX idx_a9f78616$932_action_date_pre2008_temp ON universal_award_matview_temp USING BTREE(action_date) WITH (fillfactor = 97) WHERE action_date < '2007-10-01';

REFRESH MATERIALIZED VIEW universal_award_matview_temp WITH DATA;
