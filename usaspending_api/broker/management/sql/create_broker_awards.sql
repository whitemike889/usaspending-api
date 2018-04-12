DROP FUNCTION latest_transaction_fpds(text, text, text, text);

CREATE FUNCTION latest_transaction_fpds(agency_id_in text, referenced_idv_agency_iden_in text, piid_in text, parent_award_id_in text)
RETURNS RECORD AS $$
DECLARE
    result RECORD;
BEGIN
    SELECT
        agency_id_in AS agency_id,
        referenced_idv_agency_iden_in AS referenced_idv_agency_iden,
        piid_in AS piid,
        parent_award_id_in AS parent_award_id,
        SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC)) AS total_obligation,
        SUM(COALESCE(base_and_all_options_value::NUMERIC, 0::NUMERIC)) AS base_and_all_options_value,
        MIN(NULLIF(action_date, '')::DATE) AS date_signed,
        MAX(NULLIF(action_date, '')::DATE) AS certified_date,
        MIN(NULLIF(period_of_performance_star, '')::DATE) AS period_of_performance_start_date,
        MAX(NULLIF(period_of_performance_curr, '')::DATE) AS period_of_performance_current_end_date
    FROM
        detached_award_procurement AS dap
    WHERE 
        (dap.piid = piid_in OR (piid_in IS NULL AND dap.piid IS NULL))
        AND
        (dap.parent_award_id = parent_award_id_in OR (parent_award_id_in IS NULL AND dap.parent_award_id IS NULL))
        AND
        (dap.agency_id = agency_id_in OR (agency_id_in IS NULL AND dap.agency_id IS NULL)) 
        AND 
        (dap.referenced_idv_agency_iden = referenced_idv_agency_iden_in OR (referenced_idv_agency_iden_in IS NULL AND dap.referenced_idv_agency_iden IS NULL))
    INTO
   		result;

    return result;
END;
$$  LANGUAGE plpgsql;

DROP FUNCTION latest_transaction_fabs(text, text, text);

CREATE OR REPLACE FUNCTION latest_transaction(awarding_subtier_agency_code_in text, fain_in text, uri_in text)
RETURNS RECORD AS $$
DECLARE
    result RECORD;
BEGIN
    SELECT
        awarding_subtier_agency_code_in AS awarding_sub_tier_agency_c,
        fain_in AS fain,
        uri_in AS uri,
        SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC)) AS total_obligation,
        SUM(COALESCE(original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) AS total_subsidy_cost,
        SUM(COALESCE(face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) AS total_loan_value,
        SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC) + COALESCE(non_federal_funding_amount::NUMERIC, 0::NUMERIC)) AS total_funding_amount,
        MIN(NULLIF(action_date, '')::DATE) AS date_signed,
        MAX(NULLIF(action_date, '')::DATE) AS certified_date,
        MIN(NULLIF(period_of_performance_star, '')::DATE) AS period_of_performance_start_date,
        MAX(NULLIF(period_of_performance_curr, '')::DATE) AS period_of_performance_current_end_date
    FROM
        published_award_financial_assistance AS faba
    WHERE
        (faba.awarding_sub_tier_agency_c = awarding_subtier_agency_code_in OR (awarding_subtier_agency_code_in IS NULL AND faba.awarding_sub_tier_agency_c IS NULL))
        AND
        (faba.fain = fain_in OR (fain_in IS NULL AND faba.fain IS NULL))
        AND
        (faba.uri = uri_in OR (uri_in IS NULL AND faba.uri IS NULL)) 
    INTO
   		result;

    return result;
END;
$$  LANGUAGE plpgsql;

--CREATE INDEX temp_group_fpds_idx ON detached_award_procurement(piid, parent_award_id, agency_id, referenced_idv_agency_iden);
--CREATE INDEX temp_group_fabs_idx ON published_award_financial_assistance(awarding_sub_tier_agency_c, fain, uri);

DROP TABLE IF EXISTS awards_new;

CREATE TABLE awards_new (
    generated_unique_award_id TEXT,
    type TEXT,
    type_description TEXT,
    agency_id TEXT,
    referenced_idv_agency_iden TEXT,
    referenced_idv_agency_desc TEXT,
    multiple_or_single_award_i TEXT,
    multiple_or_single_aw_desc TEXT,
    type_of_idc TEXT,
    type_of_idc_description TEXT,
    piid TEXT,
    parent_award_piid TEXT,
    fain TEXT,
    uri TEXT,
    total_obligation NUMERIC,
    total_subsidy_cost NUMERIC,
    total_loan_value NUMERIC,
    total_funding_amount NUMERIC,
    awarding_agency_code TEXT,
    awarding_agency_name TEXT,
    awarding_sub_tier_agency_c TEXT,
    awarding_sub_tier_agency_n TEXT,
    awarding_office_code TEXT,
    awarding_office_name TEXT,
    funding_agency_code TEXT,
    funding_agency_name TEXT,
    funding_sub_tier_agency_co TEXT,
    funding_sub_tier_agency_na TEXT,
    funding_office_code TEXT,
    funding_office_name TEXT,
    action_date DATE,
    date_signed DATE,
    description TEXT,
    period_of_performance_start_date DATE,
    period_of_performance_current_end_date DATE,
    potential_total_value_of_award NUMERIC,
    base_and_all_options_value NUMERIC,
    last_modified_date DATE,
    certified_date DATE,
    record_type INTEGER,
    latest_transaction_unique_id TEXT,
--    total_subaward_amount NUMERIC,
--    subaward_count INTEGER,
    pulled_from TEXT,
    product_or_service_code TEXT,
    product_or_service_co_desc TEXT,
    extent_competed TEXT,
    extent_compete_description TEXT,
    type_of_contract_pricing TEXT,
    type_of_contract_pric_desc TEXT,
    contract_award_type_desc TEXT,
    cost_or_pricing_data TEXT,
    cost_or_pricing_data_desc TEXT,
    domestic_or_foreign_entity TEXT,
    domestic_or_foreign_e_desc TEXT,
    fair_opportunity_limited_s TEXT,
    fair_opportunity_limi_desc TEXT,
    foreign_funding TEXT,
    foreign_funding_desc TEXT,
    interagency_contracting_au TEXT,
    interagency_contract_desc TEXT,
    major_program TEXT,
    price_evaluation_adjustmen TEXT,
    program_acronym TEXT,
    subcontracting_plan TEXT,
    subcontracting_plan_desc TEXT,
    multi_year_contract TEXT,
    multi_year_contract_desc TEXT,
    purchase_card_as_payment_m TEXT,
    purchase_card_as_paym_desc TEXT,
    consolidated_contract TEXT,
    consolidated_contract_desc TEXT,
    solicitation_identifier TEXT,
    solicitation_procedures TEXT,
    solicitation_procedur_desc TEXT,
    number_of_offers_received TEXT,
    other_than_full_and_open_c TEXT,
    other_than_full_and_o_desc TEXT,
    commercial_item_acquisitio TEXT,
    commercial_item_acqui_desc TEXT,
    commercial_item_test_progr TEXT,
    commercial_item_test_desc TEXT,
    evaluated_preference TEXT,
    evaluated_preference_desc TEXT,
    fed_biz_opps TEXT,
    fed_biz_opps_description TEXT,
    small_business_competitive TEXT,
    dod_claimant_program_code TEXT,
    dod_claimant_prog_cod_desc TEXT,
    program_system_or_equipmen TEXT,
    program_system_or_equ_desc TEXT,
    information_technology_com TEXT,
    information_technolog_desc TEXT,
    sea_transportation TEXT,
    sea_transportation_desc TEXT,
    clinger_cohen_act_planning TEXT,
    clinger_cohen_act_pla_desc TEXT,
    davis_bacon_act TEXT,
    davis_bacon_act_descrip TEXT,
    service_contract_act TEXT,
    service_contract_act_desc TEXT,
    walsh_healey_act TEXT,
    walsh_healey_act_descrip TEXT,
    naics TEXT,
    naics_description TEXT,
    idv_type TEXT,
    idv_type_description TEXT,
    type_set_aside TEXT,
    type_set_aside_description TEXT,
    assistance_type TEXT,
    business_funds_indicator TEXT,
    business_types TEXT,
    business_types_description TEXT,
--        business_categories TEXT[],
    cfda_number TEXT,
    cfda_title TEXT,
    sai_number TEXT,

    -- recipient data
    recipient_unique_id TEXT, -- DUNS
    recipient_name TEXT,
    parent_recipient_unique_id TEXT,

    -- business categories
    recipient_location_address_line1 TEXT,
    recipient_location_address_line2 TEXT,
    recipient_location_address_line3 TEXT,

    -- foreign province
    recipient_location_foreign_province TEXT,
    recipient_location_foreign_city_name TEXT,
    recipient_location_foreign_postal_code TEXT,

    -- country
    recipient_location_country_code TEXT,
    recipient_location_country_name TEXT,

    -- state
    recipient_location_state_code TEXT,
    recipient_location_state_name TEXT,

    -- county (NONE FOR FPDS)
    recipient_location_county_code TEXT,
    recipient_location_county_name TEXT,

    -- city
    recipient_location_city_code TEXT,
    recipient_location_city_name TEXT,

    -- zip
    recipient_location_zip5 TEXT,

    -- congressional disctrict
    recipient_location_congressional_code TEXT,

    -- ppop data
    pop_code TEXT,

    -- foreign
    pop_foreign_province TEXT,

    -- country
    pop_country_code TEXT,
    pop_country_name TEXT,

    -- state
    pop_state_code TEXT,
    pop_state_name TEXT,

    -- county
    pop_county_code TEXT,
    pop_county_name TEXT,

    -- city
    pop_city_name TEXT,

    -- zip
    pop_zip5 TEXT,

    -- congressional disctrict
    pop_congressional_code TEXT);


INSERT INTO awards_new
SELECT
    DISTINCT ON (tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
    'CONT_AW_' ||
        COALESCE(tf.agency_id,'-NONE-') || '_' ||
        COALESCE(tf.referenced_idv_agency_iden,'-NONE-') || '_' ||
        COALESCE(tf.piid,'-NONE-') || '_' ||
        COALESCE(tf.parent_award_id,'-NONE-') AS generated_unique_award_id,
    LAST_VALUE(contract_award_type) OVER w AS type,
    LAST_VALUE(contract_award_type_desc) OVER w AS type_description,
    LAST_VALUE(agency_id) OVER w AS agency_id,
    LAST_VALUE(referenced_idv_agency_iden) OVER w AS referenced_idv_agency_iden,
    LAST_VALUE(referenced_idv_agency_desc) OVER w AS referenced_idv_agency_desc,
    LAST_VALUE(multiple_or_single_award_i) OVER w AS multiple_or_single_award_i,
    LAST_VALUE(multiple_or_single_aw_desc) OVER w AS multiple_or_single_aw_desc,
    LAST_VALUE(type_of_idc) OVER w AS type_of_idc,
    LAST_VALUE(type_of_idc_description) OVER w AS type_of_idc_description,
    LAST_VALUE(piid) OVER w AS piid,
    LAST_VALUE(parent_award_id) OVER w AS parent_award_piid,
    NULL::TEXT AS fain,
    NULL::TEXT AS uri,
    SUM(COALESCE(tf.federal_action_obligation::NUMERIC, 0::NUMERIC)) OVER w AS total_obligation,
    NULL::NUMERIC AS total_subsidy_cost,
    NULL::NUMERIC AS total_loan_value,
    NULL::NUMERIC AS total_funding_amount,
    LAST_VALUE(awarding_agency_code) OVER w,
    LAST_VALUE(awarding_agency_name) OVER w,
    LAST_VALUE(awarding_sub_tier_agency_c) OVER w,
    LAST_VALUE(awarding_sub_tier_agency_n) OVER w,
    LAST_VALUE(awarding_office_code) OVER w,
    LAST_VALUE(awarding_office_name) OVER w,
    LAST_VALUE(funding_agency_code) OVER w,
    LAST_VALUE(funding_agency_name) OVER w,
    LAST_VALUE(funding_sub_tier_agency_co) OVER w AS funding_sub_tier_agency_co,
    LAST_VALUE(funding_sub_tier_agency_na) OVER w AS funding_sub_tier_agency_na,
    LAST_VALUE(funding_office_code) OVER w AS funding_office_code,
    LAST_VALUE(funding_office_name) OVER w AS funding_office_name,
    NULLIF(LAST_VALUE(action_date) OVER w, '')::DATE AS action_date,
    MIN(NULLIF(tf.action_date, '')::DATE) OVER w AS date_signed,
    LAST_VALUE(award_description) OVER w AS description,
    MIN(NULLIF(tf.period_of_performance_star, '')::DATE) OVER w AS period_of_performance_start_date,
    MAX(NULLIF(tf.period_of_performance_curr, '')::DATE) OVER w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    SUM(COALESCE(tf.base_and_all_options_value::NUMERIC, 0::NUMERIC)) OVER w AS base_and_all_options_value,
    LAST_VALUE(last_modified::DATE) OVER w AS last_modified_date,
    MAX(NULLIF(tf.action_date, '')::DATE) OVER w AS certified_date,
    NULL::INTEGER AS record_type,
    LAST_VALUE(detached_award_proc_unique) OVER w AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    LAST_VALUE(pulled_from) OVER w AS pulled_from,
    LAST_VALUE(product_or_service_code) OVER w AS product_or_service_code,
    LAST_VALUE(product_or_service_co_desc) OVER w AS product_or_service_co_desc,
    LAST_VALUE(extent_competed) OVER w AS extent_competed,
    LAST_VALUE(extent_compete_description) OVER w AS extent_compete_description,
    LAST_VALUE(type_of_contract_pricing) OVER w AS type_of_contract_pricing,
    LAST_VALUE(type_of_contract_pric_desc) OVER w AS type_of_contract_pric_desc,
    LAST_VALUE(contract_award_type_desc) OVER w AS contract_award_type_desc,
    LAST_VALUE(cost_or_pricing_data) OVER w AS cost_or_pricing_data,
    LAST_VALUE(cost_or_pricing_data_desc) OVER w AS cost_or_pricing_data_desc,
    LAST_VALUE(domestic_or_foreign_entity) OVER w AS domestic_or_foreign_entity,
    LAST_VALUE(domestic_or_foreign_e_desc) OVER w AS domestic_or_foreign_e_desc,
    LAST_VALUE(fair_opportunity_limited_s) OVER w AS fair_opportunity_limited_s,
    LAST_VALUE(fair_opportunity_limi_desc) OVER w AS fair_opportunity_limi_desc,
    LAST_VALUE(foreign_funding) OVER w AS foreign_funding,
    LAST_VALUE(foreign_funding_desc) OVER w AS foreign_funding_desc,
    LAST_VALUE(interagency_contracting_au) OVER w AS interagency_contracting_au,
    LAST_VALUE(interagency_contract_desc) OVER w AS interagency_contract_desc,
    LAST_VALUE(major_program) OVER w AS major_program,
    LAST_VALUE(price_evaluation_adjustmen) OVER w AS price_evaluation_adjustmen,
    LAST_VALUE(program_acronym) OVER w AS program_acronym,
    LAST_VALUE(subcontracting_plan) OVER w AS subcontracting_plan,
    LAST_VALUE(subcontracting_plan_desc) OVER w AS subcontracting_plan_desc,
    LAST_VALUE(multi_year_contract) OVER w AS multi_year_contract,
    LAST_VALUE(multi_year_contract_desc) OVER w AS multi_year_contract_desc,
    LAST_VALUE(purchase_card_as_payment_m) OVER w AS purchase_card_as_payment_m,
    LAST_VALUE(purchase_card_as_paym_desc) OVER w AS purchase_card_as_paym_desc,
    LAST_VALUE(consolidated_contract) OVER w AS consolidated_contract,
    LAST_VALUE(consolidated_contract_desc) OVER w AS consolidated_contract_desc,
    LAST_VALUE(solicitation_identifier) OVER w AS solicitation_identifier,
    LAST_VALUE(solicitation_procedures) OVER w AS solicitation_procedures,
    LAST_VALUE(solicitation_procedur_desc) OVER w AS solicitation_procedur_desc,
    LAST_VALUE(number_of_offers_received) OVER w AS number_of_offers_received,
    LAST_VALUE(other_than_full_and_open_c) OVER w AS other_than_full_and_open_c,
    LAST_VALUE(other_than_full_and_o_desc) OVER w AS other_than_full_and_o_desc,
    LAST_VALUE(commercial_item_acquisitio) OVER w AS commercial_item_acquisitio,
    LAST_VALUE(commercial_item_acqui_desc) OVER w AS commercial_item_acqui_desc,
    LAST_VALUE(commercial_item_test_progr) OVER w AS commercial_item_test_progr,
    LAST_VALUE(commercial_item_test_desc) OVER w AS commercial_item_test_desc,
    LAST_VALUE(evaluated_preference) OVER w AS evaluated_preference,
    LAST_VALUE(evaluated_preference_desc) OVER w AS evaluated_preference_desc,
    LAST_VALUE(fed_biz_opps) OVER w AS fed_biz_opps,
    LAST_VALUE(fed_biz_opps_description) OVER w AS fed_biz_opps_description,
    LAST_VALUE(small_business_competitive) OVER w AS small_business_competitive,
    LAST_VALUE(dod_claimant_program_code) OVER w AS dod_claimant_program_code,
    LAST_VALUE(dod_claimant_prog_cod_desc) OVER w AS dod_claimant_prog_cod_desc,
    LAST_VALUE(program_system_or_equipmen) OVER w AS program_system_or_equipmen,
    LAST_VALUE(program_system_or_equ_desc) OVER w AS program_system_or_equ_desc,
    LAST_VALUE(information_technology_com) OVER w AS information_technology_com,
    LAST_VALUE(information_technolog_desc) OVER w AS information_technolog_desc,
    LAST_VALUE(sea_transportation) OVER w AS sea_transportation,
    LAST_VALUE(sea_transportation_desc) OVER w AS sea_transportation_desc,
    LAST_VALUE(clinger_cohen_act_planning) OVER w AS clinger_cohen_act_planning,
    LAST_VALUE(clinger_cohen_act_pla_desc) OVER w AS clinger_cohen_act_pla_desc,
    LAST_VALUE(davis_bacon_act) OVER w AS davis_bacon_act,
    LAST_VALUE(davis_bacon_act_descrip) OVER w AS davis_bacon_act_descrip,
    LAST_VALUE(service_contract_act) OVER w AS service_contract_act,
    LAST_VALUE(service_contract_act_desc) OVER w AS service_contract_act_desc,
    LAST_VALUE(walsh_healey_act) OVER w AS walsh_healey_act,
    LAST_VALUE(walsh_healey_act_descrip) OVER w AS walsh_healey_act_descrip,
    LAST_VALUE(naics) OVER w AS naics,
    LAST_VALUE(naics_description) OVER w AS naics_description,
    LAST_VALUE(idv_type) OVER w AS idv_type,
    LAST_VALUE(idv_type_description) OVER w AS idv_type_description,
    LAST_VALUE(type_set_aside) OVER w AS type_set_aside,
    LAST_VALUE(type_set_aside_description) OVER w AS type_set_aside_description,
    NULL::TEXT AS assistance_type,
    NULL::TEXT AS business_funds_indicator,
    NULL::TEXT AS business_types,
    NULL::TEXT AS business_types_description,
    -- business_categories,
    NULL::TEXT AS cfda_number,
    NULL::TEXT AS cfda_title,
    NULL::TEXT AS sai_number,

    -- recipient data
    LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id, -- DUNS
    LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
    LAST_VALUE(ultimate_parent_unique_ide) OVER w AS parent_recipient_unique_id,

    -- business categories
    LAST_VALUE(legal_entity_address_line1) OVER w AS recipient_location_address_line1,
    LAST_VALUE(legal_entity_address_line2) OVER w AS recipient_location_address_line2,
    LAST_VALUE(legal_entity_address_line3) OVER w AS recipient_location_address_line3,

    -- foreign province
    NULL::TEXT AS recipient_location_foreign_province,
    NULL::TEXT AS recipient_location_foreign_city_name,
    NULL::TEXT AS recipient_location_foreign_city_name,

    -- country
    LAST_VALUE(legal_entity_country_code) OVER w AS recipient_location_country_code,
    LAST_VALUE(legal_entity_country_name) OVER w AS recipient_location_country_name,

    -- state
    LAST_VALUE(legal_entity_state_code) OVER w AS recipient_location_state_code,
    LAST_VALUE(legal_entity_state_descrip) OVER w AS recipient_location_state_name,

    -- county
    LAST_VALUE(legal_entity_county_code) OVER w AS recipient_location_county_code,
    LAST_VALUE(legal_entity_county_name) OVER w AS recipient_location_county_name,

    -- city
    NULL::TEXT AS recipient_location_city_code,
    LAST_VALUE(legal_entity_city_name) OVER w AS recipient_location_city_name,

    -- zip
    LAST_VALUE(legal_entity_zip5) OVER w AS recipient_location_zip5,

    -- congressional disctrict
    LAST_VALUE(legal_entity_congressional) OVER w AS recipient_location_congressional_code,

    -- ppop data
    NULL::TEXT AS pop_code,

    -- foreign
    NULL::TEXT AS pop_foreign_province,

    -- country
    LAST_VALUE(place_of_perform_country_c) OVER w AS pop_country_code,
    LAST_VALUE(place_of_perf_country_desc) OVER w AS pop_country_name,

    -- state
    LAST_VALUE(place_of_performance_state) OVER w AS pop_state_code,
    LAST_VALUE(place_of_perfor_state_desc) OVER w AS pop_state_name,

    -- county
    LAST_VALUE(place_of_perform_county_co) OVER w AS pop_county_code,
    LAST_VALUE(place_of_perform_county_na) OVER w AS pop_county_name,

    -- city
    LAST_VALUE(place_of_perform_city_name) OVER w AS pop_city_name,

    -- zip
    LAST_VALUE(place_of_performance_zip5) OVER w AS pop_zip5,

    -- congressional disctrict
    LAST_VALUE(place_of_performance_congr) OVER w AS pop_congressional_code
    FROM
    detached_award_procurement AS tf
    --        LEFT OUTER JOIN
    --        exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = tf.awardee_or_recipient_uniqu
    WINDOW w AS (PARTITION BY tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
    ORDER BY
    tf.piid,
    tf.parent_award_id,
    tf.agency_id,
    tf.referenced_idv_agency_iden,
    tf.action_date DESC,
    tf.award_modification_amendme DESC,
    tf.transaction_number DESC;


INSERT INTO awards_new
SELECT
    DISTINCT ON (pafa.fain, pafa.awarding_sub_tier_agency_c)
    'ASST_AW_' ||
        COALESCE(pafa.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
        COALESCE(pafa.fain, '-NONE-') || '_' ||
        '-NONE-' AS generated_unique_award_id,
    LAST_VALUE(assistance_type) OVER w AS type,
    CASE
        WHEN LAST_VALUE(assistance_type) OVER w = '02' THEN 'Block Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '03' THEN 'Formula Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '04' THEN 'Project Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '05' THEN 'Cooperative Agreement'
        WHEN LAST_VALUE(assistance_type) OVER w = '06' THEN 'Direct Payment for Specified Use'
        WHEN LAST_VALUE(assistance_type) OVER w = '07' THEN 'Direct Loan'
        WHEN LAST_VALUE(assistance_type) OVER w = '08' THEN 'Guaranteed/Insured Loan'
        WHEN LAST_VALUE(assistance_type) OVER w = '09' THEN 'Insurance'
        WHEN LAST_VALUE(assistance_type) OVER w = '10' THEN 'Direct Payment with Unrestricted Use'
        WHEN LAST_VALUE(assistance_type) OVER w = '11' THEN 'Other Financial Assistance'
    END AS type_description,
    NULL::TEXT AS agency_id,
    NULL::TEXT AS referenced_idv_agency_iden,
    NULL::TEXT AS referenced_idv_agency_desc,
    NULL::TEXT AS multiple_or_single_award_i,
    NULL::TEXT AS multiple_or_single_aw_desc,
    NULL::TEXT AS type_of_idc,
    NULL::TEXT AS type_of_idc_description,
    NULL::TEXT AS piid,
    NULL::TEXT AS parent_award_piid,
    LAST_VALUE(fain) OVER w AS fain,
    NULL::TEXT AS uri,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC)) OVER w AS total_obligation,
    SUM(COALESCE(pafa.original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) OVER w AS total_subsidy_cost,
    SUM(COALESCE(pafa.face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) OVER w AS total_loan_value,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC) + COALESCE(pafa.non_federal_funding_amount::NUMERIC, 0::NUMERIC)) OVER w AS total_funding_amount,
    LAST_VALUE(awarding_agency_code) OVER w AS awarding_agency_code,
    LAST_VALUE(awarding_agency_name) OVER w AS awarding_agency_name,
    LAST_VALUE(awarding_sub_tier_agency_c) OVER w AS awarding_sub_tier_agency_c,
    LAST_VALUE(awarding_sub_tier_agency_n) OVER w AS awarding_sub_tier_agency_n,
    LAST_VALUE(awarding_office_code) OVER w AS awarding_office_code,
    LAST_VALUE(awarding_office_name) OVER w AS awarding_office_name,
    LAST_VALUE(funding_agency_code) OVER w AS funding_agency_code,
    LAST_VALUE(funding_agency_name) OVER w AS funding_agency_name,
    LAST_VALUE(funding_sub_tier_agency_co) OVER w AS funding_sub_tier_agency_co,
    LAST_VALUE(funding_sub_tier_agency_na) OVER w AS funding_sub_tier_agency_na,
    LAST_VALUE(funding_office_code) OVER w AS funding_office_code,
    LAST_VALUE(funding_office_name) OVER w AS funding_office_name,
    NULLIF(LAST_VALUE(action_date) OVER w, '')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '')::DATE) OVER w AS date_signed,
    LAST_VALUE(award_description) OVER w AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is '
    MIN(NULLIF(pafa.period_of_performance_star, '')::DATE) OVER w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '')::DATE) OVER w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    LAST_VALUE(modified_at::DATE) OVER w AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '')::DATE) OVER w AS certified_date,
    LAST_VALUE(record_type) OVER w AS record_type,
    LAST_VALUE(afa_generated_unique) OVER w AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    NULL::TEXT AS pulled_from,
    NULL::TEXT AS product_or_service_code,
    NULL::TEXT AS product_or_service_co_desc,
    NULL::TEXT AS extent_competed,
    NULL::TEXT AS extent_compete_description,
    NULL::TEXT AS type_of_contract_pricing,
    NULL::TEXT AS type_of_contract_pric_desc,
    NULL::TEXT AS contract_award_type_desc,
    NULL::TEXT AS cost_or_pricing_data,
    NULL::TEXT AS cost_or_pricing_data_desc,
    NULL::TEXT AS domestic_or_foreign_entity,
    NULL::TEXT AS domestic_or_foreign_e_desc,
    NULL::TEXT AS fair_opportunity_limited_s,
    NULL::TEXT AS fair_opportunity_limi_desc,
    NULL::TEXT AS foreign_funding,
    NULL::TEXT AS foreign_funding_desc,
    NULL::TEXT AS interagency_contracting_au,
    NULL::TEXT AS interagency_contract_desc,
    NULL::TEXT AS major_program,
    NULL::TEXT AS price_evaluation_adjustmen,
    NULL::TEXT AS program_acronym,
    NULL::TEXT AS subcontracting_plan,
    NULL::TEXT AS subcontracting_plan_desc,
    NULL::TEXT AS multi_year_contract,
    NULL::TEXT AS multi_year_contract_desc,
    NULL::TEXT AS purchase_card_as_payment_m,
    NULL::TEXT AS purchase_card_as_paym_desc,
    NULL::TEXT AS consolidated_contract,
    NULL::TEXT AS consolidated_contract_desc,
    NULL::TEXT AS solicitation_identifier,
    NULL::TEXT AS solicitation_procedures,
    NULL::TEXT AS solicitation_procedur_desc,
    NULL::TEXT AS number_of_offers_received,
    NULL::TEXT AS other_than_full_and_open_c,
    NULL::TEXT AS other_than_full_and_o_desc,
    NULL::TEXT AS commercial_item_acquisitio,
    NULL::TEXT AS commercial_item_acqui_desc,
    NULL::TEXT AS commercial_item_test_progr,
    NULL::TEXT AS commercial_item_test_desc,
    NULL::TEXT AS evaluated_preference,
    NULL::TEXT AS evaluated_preference_desc,
    NULL::TEXT AS fed_biz_opps,
    NULL::TEXT AS fed_biz_opps_description,
    NULL::TEXT AS small_business_competitive,
    NULL::TEXT AS dod_claimant_program_code,
    NULL::TEXT AS dod_claimant_prog_cod_desc,
    NULL::TEXT AS program_system_or_equipmen,
    NULL::TEXT AS program_system_or_equ_desc,
    NULL::TEXT AS information_technology_com,
    NULL::TEXT AS information_technolog_desc,
    NULL::TEXT AS sea_transportation,
    NULL::TEXT AS sea_transportation_desc,
    NULL::TEXT AS clinger_cohen_act_planning,
    NULL::TEXT AS clinger_cohen_act_pla_desc,
    NULL::TEXT AS davis_bacon_act,
    NULL::TEXT AS davis_bacon_act_descrip,
    NULL::TEXT AS service_contract_act,
    NULL::TEXT AS service_contract_act_desc,
    NULL::TEXT AS walsh_healey_act,
    NULL::TEXT AS walsh_healey_act_descrip,
    NULL::TEXT AS naics,
    NULL::TEXT AS naics_description,
    NULL::TEXT AS idv_type,
    NULL::TEXT AS idv_type_description,
    NULL::TEXT AS type_set_aside,
    NULL::TEXT AS type_set_aside_description,
    LAST_VALUE(assistance_type) OVER w AS assistance_type,
    LAST_VALUE(business_funds_indicator) OVER w AS business_funds_indicator,
    LAST_VALUE(business_types) OVER w AS business_types,
    CASE
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'A' THEN 'State government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'B' THEN 'County Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'C' THEN 'City or Township Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'D' THEN 'Special District Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'E' THEN 'Regional Organization'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'F' THEN 'U.S. Territory or Possession'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'G' THEN 'Independent School District'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'L' THEN 'Public/Indian Housing Authority'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'O' THEN 'Private Institution of Higher Education'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'P' THEN 'Individual'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'R' THEN 'Small Business'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'S' THEN 'Hispanic-serving Institution'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'W' THEN 'Non-domestic (non-US) Entity'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'X' THEN 'Other'
        ELSE 'Unknown Types'
    END AS business_types_description,
--    compile_fabs_business_categories(LAST_VALUE(business_types) OVER w) AS business_categories,
    LAST_VALUE(cfda_number) OVER w AS cfda_number,
    LAST_VALUE(cfda_title) OVER w AS cfda_title,
    LAST_VALUE(sai_number) OVER w AS sai_number,

    -- recipient data
    LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id,
    LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
    NULL::TEXT AS parent_recipient_unique_id,

    -- business categories
    LAST_VALUE(legal_entity_address_line1) OVER w AS recipient_location_address_line1,
    LAST_VALUE(legal_entity_address_line2) OVER w AS recipient_location_address_line2,
    LAST_VALUE(legal_entity_address_line3) OVER w AS recipient_location_address_line3,

    -- foreign province
    LAST_VALUE(legal_entity_foreign_provi) OVER w AS recipient_location_foreign_province,
    LAST_VALUE(legal_entity_foreign_city) OVER w AS recipient_location_foreign_city_name,
    LAST_VALUE(legal_entity_foreign_posta) OVER w AS recipient_location_foreign_postal_code,

    -- country
    LAST_VALUE(legal_entity_country_code) OVER w AS recipient_location_country_code,
    LAST_VALUE(legal_entity_country_name) OVER w AS recipient_location_country_name,

    -- state
    LAST_VALUE(legal_entity_state_code) OVER w AS recipient_location_state_code,
    LAST_VALUE(legal_entity_state_name) OVER w AS recipient_location_state_name,

    -- county
    LAST_VALUE(legal_entity_county_code) OVER w AS recipient_location_county_code,
    LAST_VALUE(legal_entity_county_name) OVER w AS recipient_location_county_name,

    -- city
    LAST_VALUE(legal_entity_city_code) OVER w AS recipient_location_city_code,
    LAST_VALUE(legal_entity_city_name) OVER w AS recipient_location_city_name,

    -- zip
    LAST_VALUE(legal_entity_zip5) OVER w AS recipient_location_zip5,

    -- congressional disctrict
    LAST_VALUE(legal_entity_congressional) OVER w AS recipient_location_congressional_code,

    -- ppop data
    LAST_VALUE(place_of_performance_code) OVER w AS pop_code,

    -- foreign
    LAST_VALUE(place_of_performance_forei) OVER w AS pop_foreign_province,

    -- country
    LAST_VALUE(place_of_perform_country_c) OVER w AS pop_country_code,
    LAST_VALUE(place_of_perform_country_n) OVER w AS pop_country_name,

    -- state
    LAST_VALUE(place_of_perfor_state_code) OVER w AS pop_state_code,
    LAST_VALUE(place_of_perform_state_nam) OVER w AS pop_state_name,

    -- county
    LAST_VALUE(place_of_perform_county_co) OVER w AS pop_county_code,
    LAST_VALUE(place_of_perform_county_na) OVER w AS pop_county_name,

    -- city
    LAST_VALUE(place_of_performance_city) OVER w AS pop_city_name,

    -- zip
    LAST_VALUE(place_of_performance_zip5) OVER w AS pop_zip5,

    -- congressional disctrict
    LAST_VALUE(place_of_performance_congr) OVER w AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
--    LEFT OUTER JOIN
--    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = LAST_VALUE(pafa.awardee_or_recipient_uniqu) OVER w
WHERE pafa.record_type = '2' AND is_active IS TRUE
WINDOW w AS (PARTITION BY pafa.fain, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.fain,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date DESC,
    pafa.award_modification_amendme DESC
;

INSERT INTO awards_new
SELECT
    DISTINCT ON (pafa.uri, pafa.awarding_sub_tier_agency_c)
    'ASST_AW' ||
        COALESCE(pafa.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
        '-NONE-' || '_' ||
        COALESCE(pafa.uri, '-NONE-') AS generated_unique_award_id,
    LAST_VALUE(assistance_type) OVER w AS type,
    CASE
        WHEN LAST_VALUE(assistance_type) OVER w = '02' THEN 'Block Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '03' THEN 'Formula Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '04' THEN 'Project Grant'
        WHEN LAST_VALUE(assistance_type) OVER w = '05' THEN 'Cooperative Agreement'
        WHEN LAST_VALUE(assistance_type) OVER w = '06' THEN 'Direct Payment for Specified Use'
        WHEN LAST_VALUE(assistance_type) OVER w = '07' THEN 'Direct Loan'
        WHEN LAST_VALUE(assistance_type) OVER w = '08' THEN 'Guaranteed/Insured Loan'
        WHEN LAST_VALUE(assistance_type) OVER w = '09' THEN 'Insurance'
        WHEN LAST_VALUE(assistance_type) OVER w = '10' THEN 'Direct Payment with Unrestricted Use'
        WHEN LAST_VALUE(assistance_type) OVER w = '11' THEN 'Other Financial Assistance'
    END AS type_description,
    NULL::text AS agency_id,
    NULL::text AS referenced_idv_agency_iden,
    NULL::text AS referenced_idv_agency_desc,
    NULL::text AS multiple_or_single_award_i,
    NULL::text AS multiple_or_single_aw_desc,
    NULL::text AS type_of_idc,
    NULL::text AS type_of_idc_description,
    NULL::text AS piid,
    NULL::text AS parent_award_piid,
    NULL::text AS fain,
    LAST_VALUE(uri) OVER w AS uri,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC)) OVER w AS total_obligation,
    SUM(COALESCE(pafa.original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) OVER w AS total_subsidy_cost,
    SUM(COALESCE(pafa.face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) OVER w AS total_loan_value,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC) + COALESCE(pafa.non_federal_funding_amount::NUMERIC, 0::NUMERIC)) OVER w AS total_funding_amount,
    LAST_VALUE(awarding_agency_code) OVER w AS awarding_agency_code,
    LAST_VALUE(awarding_agency_name) OVER w AS awarding_agency_name,
    LAST_VALUE(awarding_sub_tier_agency_c) OVER w AS awarding_sub_tier_agency_c,
    LAST_VALUE(awarding_sub_tier_agency_n) OVER w AS awarding_sub_tier_agency_n,
    LAST_VALUE(awarding_office_code) OVER w AS awarding_office_code,
    LAST_VALUE(awarding_office_name) OVER w AS awarding_office_name,
    LAST_VALUE(funding_agency_code) OVER w AS funding_agency_code,
    LAST_VALUE(funding_agency_name) OVER w AS funding_agency_name,
    LAST_VALUE(funding_sub_tier_agency_co) OVER w AS funding_sub_tier_agency_co,
    LAST_VALUE(funding_sub_tier_agency_na) OVER w AS funding_sub_tier_agency_na,
    LAST_VALUE(funding_office_code) OVER w AS funding_office_code,
    LAST_VALUE(funding_office_name) OVER w AS funding_office_name,
    NULLIF(LAST_VALUE(action_date) OVER w, '')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '')::DATE) OVER w AS date_signed,
    LAST_VALUE(award_description) OVER w AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is '
    MIN(NULLIF(pafa.period_of_performance_star, '')::DATE) OVER w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '')::DATE) OVER w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    LAST_VALUE(modified_at::DATE) OVER w AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '')::DATE) OVER w AS certified_date,
    LAST_VALUE(record_type) OVER w AS record_type,
    LAST_VALUE(afa_generated_unique) OVER w AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    NULL::text AS pulled_from,
    NULL::text AS product_or_service_code,
    NULL::text AS product_or_service_co_desc,
    NULL::text AS extent_competed,
    NULL::text AS extent_compete_description,
    NULL::text AS type_of_contract_pricing,
    NULL::text AS type_of_contract_pric_desc,
    NULL::text AS contract_award_type_desc,
    NULL::text AS cost_or_pricing_data,
    NULL::text AS cost_or_pricing_data_desc,
    NULL::text AS domestic_or_foreign_entity,
    NULL::text AS domestic_or_foreign_e_desc,
    NULL::text AS fair_opportunity_limited_s,
    NULL::text AS fair_opportunity_limi_desc,
    NULL::text AS foreign_funding,
    NULL::text AS foreign_funding_desc,
    NULL::text AS interagency_contracting_au,
    NULL::text AS interagency_contract_desc,
    NULL::text AS major_program,
    NULL::text AS price_evaluation_adjustmen,
    NULL::text AS program_acronym,
    NULL::text AS subcontracting_plan,
    NULL::text AS subcontracting_plan_desc,
    NULL::text AS multi_year_contract,
    NULL::text AS multi_year_contract_desc,
    NULL::text AS purchase_card_as_payment_m,
    NULL::text AS purchase_card_as_paym_desc,
    NULL::text AS consolidated_contract,
    NULL::text AS consolidated_contract_desc,
    NULL::text AS solicitation_identifier,
    NULL::text AS solicitation_procedures,
    NULL::text AS solicitation_procedur_desc,
    NULL::text AS number_of_offers_received,
    NULL::text AS other_than_full_and_open_c,
    NULL::text AS other_than_full_and_o_desc,
    NULL::text AS commercial_item_acquisitio,
    NULL::text AS commercial_item_acqui_desc,
    NULL::text AS commercial_item_test_progr,
    NULL::text AS commercial_item_test_desc,
    NULL::text AS evaluated_preference,
    NULL::text AS evaluated_preference_desc,
    NULL::text AS fed_biz_opps,
    NULL::text AS fed_biz_opps_description,
    NULL::text AS small_business_competitive,
    NULL::text AS dod_claimant_program_code,
    NULL::text AS dod_claimant_prog_cod_desc,
    NULL::text AS program_system_or_equipmen,
    NULL::text AS program_system_or_equ_desc,
    NULL::text AS information_technology_com,
    NULL::text AS information_technolog_desc,
    NULL::text AS sea_transportation,
    NULL::text AS sea_transportation_desc,
    NULL::text AS clinger_cohen_act_planning,
    NULL::text AS clinger_cohen_act_pla_desc,
    NULL::text AS davis_bacon_act,
    NULL::text AS davis_bacon_act_descrip,
    NULL::text AS service_contract_act,
    NULL::text AS service_contract_act_desc,
    NULL::text AS walsh_healey_act,
    NULL::text AS walsh_healey_act_descrip,
    NULL::text AS naics,
    NULL::text AS naics_description,
    NULL::text AS idv_type,
    NULL::text AS idv_type_description,
    NULL::text AS type_set_aside,
    NULL::text AS type_set_aside_description,
    LAST_VALUE(assistance_type) OVER w AS assistance_type,
    LAST_VALUE(business_funds_indicator) OVER w AS business_funds_indicator,
    LAST_VALUE(business_types) OVER w AS business_types,
    CASE
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'A' THEN 'State government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'B' THEN 'County Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'C' THEN 'City or Township Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'D' THEN 'Special District Government'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'E' THEN 'Regional Organization'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'F' THEN 'U.S. Territory or Possession'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'G' THEN 'Independent School District'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'L' THEN 'Public/Indian Housing Authority'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'O' THEN 'Private Institution of Higher Education'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'P' THEN 'Individual'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'R' THEN 'Small Business'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'S' THEN 'Hispanic-serving Institution'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'W' THEN 'Non-domestic (non-US) Entity'
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = 'X' THEN 'Other'
        ELSE 'Unknown Types'
    END AS business_types_description,
--    compile_fabs_business_categories(LAST_VALUE(business_types) OVER w) AS business_categories,
    LAST_VALUE(cfda_number) OVER w AS cfda_number,
    LAST_VALUE(cfda_title) OVER w AS cfda_title,
    LAST_VALUE(sai_number) OVER w AS sai_number,

    -- recipient data
    LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id,
    LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- business categories
    LAST_VALUE(legal_entity_address_line1) OVER w AS recipient_location_address_line1,
    LAST_VALUE(legal_entity_address_line2) OVER w AS recipient_location_address_line2,
    LAST_VALUE(legal_entity_address_line3) OVER w AS recipient_location_address_line3,

    -- foreign province
    LAST_VALUE(legal_entity_foreign_provi) OVER w AS recipient_location_foreign_province,
    LAST_VALUE(legal_entity_foreign_city) OVER w AS recipient_location_foreign_city_name,
    LAST_VALUE(legal_entity_foreign_posta) OVER w AS recipient_location_foreign_postal_code,

    -- country
    LAST_VALUE(legal_entity_country_code) OVER w AS recipient_location_country_code,
    LAST_VALUE(legal_entity_country_name) OVER w AS recipient_location_country_name,

    -- state
    LAST_VALUE(legal_entity_state_code) OVER w AS recipient_location_state_code,
    LAST_VALUE(legal_entity_state_name) OVER w AS recipient_location_state_name,

    -- county
    LAST_VALUE(legal_entity_county_code) OVER w AS recipient_location_county_code,
    LAST_VALUE(legal_entity_county_name) OVER w AS recipient_location_county_name,

    -- city
    LAST_VALUE(legal_entity_city_code) OVER w AS recipient_location_city_code,
    LAST_VALUE(legal_entity_city_name) OVER w AS recipient_location_city_name,

    -- zip
    LAST_VALUE(legal_entity_zip5) OVER w AS recipient_location_zip5,

    -- congressional disctrict
    LAST_VALUE(legal_entity_congressional) OVER w AS recipient_location_congressional_code,

    -- ppop data
    LAST_VALUE(place_of_performance_code) OVER w AS pop_code,

    -- foreign
    LAST_VALUE(place_of_performance_forei) OVER w AS pop_foreign_province,

    -- country
    LAST_VALUE(place_of_perform_country_c) OVER w AS pop_country_code,
    LAST_VALUE(place_of_perform_country_n) OVER w AS pop_country_name,

    -- state
    LAST_VALUE(place_of_perfor_state_code) OVER w AS pop_state_code,
    LAST_VALUE(place_of_perform_state_nam) OVER w AS pop_state_name,

    -- county
    LAST_VALUE(place_of_perform_county_co) OVER w AS pop_county_code,
    LAST_VALUE(place_of_perform_county_na) OVER w AS pop_county_name,

    -- city
    LAST_VALUE(place_of_performance_city) OVER w AS pop_city_name,

    -- zip
    LAST_VALUE(place_of_performance_zip5) OVER w AS pop_zip5,

    -- congressional disctrict
    LAST_VALUE(place_of_performance_congr) OVER w AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
--    LEFT OUTER JOIN
--    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = LAST_VALUE(pafa.awardee_or_recipient_uniqu) OVER w
WHERE pafa.record_type = '1' AND is_active IS TRUE
WINDOW w AS (PARTITION BY pafa.uri, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.uri,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date DESC,
    pafa.award_modification_amendme DESC;