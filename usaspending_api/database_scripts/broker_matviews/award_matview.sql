DROP MATERIALIZED VIEW IF EXISTS award_matview_new;
DROP MATERIALIZED VIEW IF EXISTS award_matview_old;

CREATE MATERIALIZED VIEW award_matview_new AS (
(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fpds_uniq_awards.agency_id AS agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    type_of_idc,
    type_of_idc_description,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_loan_value,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c AS awarding_subtier_agency_code,
    awarding_sub_tier_agency_n AS awarding_subtier_agency_name,
    awarding_agency.subtier_abbr AS awarding_subtier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co AS funding_subtier_agency_code,
    funding_sub_tier_agency_na AS funding_subtier_agency_name,
    funding_agency.subtier_abbr AS funding_subtier_agency_abbr,
    funding_office_code,
    funding_office_name,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    business_types_description,
--    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    NULL::text AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
--    officer_1_name,
--    officer_1_amount,
--    officer_2_name,
--    officer_2_amount,
--    officer_3_name,
--    officer_3_amount,
--    officer_4_name,
--    officer_4_amount,
--    officer_5_name,
--    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month'))::INT AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
        DISTINCT ON (tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
        ''CONT_AW_'' ||
            COALESCE(tf.agency_id,''-NONE-'') || ''_'' ||
            COALESCE(tf.referenced_idv_agency_iden,''-NONE-'') || ''_'' ||
            COALESCE(tf.piid,''-NONE-'') || ''_'' ||
            COALESCE(tf.parent_award_id,''-NONE-'') AS generated_unique_award_id,
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
        NULL::text AS fain,
        NULL::text AS uri,
        SUM(COALESCE(tf.federal_action_obligation::NUMERIC, 0::NUMERIC)) OVER w AS total_obligation,
        NULL::NUMERIC AS total_subsidy_cost,
        NULL::NUMERIC AS total_loan_value,
        NULL::NUMERIC AS total_outlay,
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
        NULLIF(LAST_VALUE(action_date) OVER w, '''')::DATE AS action_date,
        MIN(NULLIF(tf.action_date, '''')::DATE) OVER w AS date_signed,
        LAST_VALUE(award_description) OVER w AS description,
        MIN(NULLIF(tf.period_of_performance_star, '''')::DATE) OVER w AS period_of_performance_start_date,
        MAX(NULLIF(tf.period_of_performance_curr, '''')::DATE) OVER w AS period_of_performance_current_end_date,
        NULL::NUMERIC AS potential_total_value_of_award,
        SUM(COALESCE(tf.base_and_all_options_value::NUMERIC, 0::NUMERIC)) OVER w AS base_and_all_options_value,
        LAST_VALUE(last_modified::DATE) OVER w AS last_modified_date,
        MAX(NULLIF(tf.action_date, '''')::DATE) OVER w AS certified_date,
        NULL::int AS record_type,
        LAST_VALUE(detached_award_proc_unique) OVER w AS latest_transaction_unique_id,
        0 AS total_subaward_amount,
        0 AS subaward_count,
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
        NULL::text AS assistance_type,
        NULL::text AS business_funds_indicator,
        NULL::text AS business_types,
        NULL::text AS business_types_description,
--        compile_fpds_business_categories(
--            LAST_VALUE(contracting_officers_deter) OVER w,
--            LAST_VALUE(corporate_entity_tax_exemp) OVER w,
--            LAST_VALUE(corporate_entity_not_tax_e) OVER w,
--            LAST_VALUE(partnership_or_limited_lia) OVER w,
--            LAST_VALUE(sole_proprietorship) OVER w,
--            LAST_VALUE(manufacturer_of_goods) OVER w,
--            LAST_VALUE(subchapter_s_corporation) OVER w,
--            LAST_VALUE(limited_liability_corporat) OVER w,
--            LAST_VALUE(for_profit_organization) OVER w,
--            LAST_VALUE(alaskan_native_owned_corpo) OVER w,
--            LAST_VALUE(american_indian_owned_busi) OVER w,
--            LAST_VALUE(asian_pacific_american_own) OVER w,
--            LAST_VALUE(black_american_owned_busin) OVER w,
--            LAST_VALUE(hispanic_american_owned_bu) OVER w,
--            LAST_VALUE(native_american_owned_busi) OVER w,
--            LAST_VALUE(native_hawaiian_owned_busi) OVER w,
--            LAST_VALUE(subcontinent_asian_asian_i) OVER w,
--            LAST_VALUE(tribally_owned_business) OVER w,
--            LAST_VALUE(other_minority_owned_busin) OVER w,
--            LAST_VALUE(minority_owned_business) OVER w,
--            LAST_VALUE(women_owned_small_business) OVER w,
--            LAST_VALUE(economically_disadvantaged) OVER w,
--            LAST_VALUE(joint_venture_women_owned) OVER w,
--            LAST_VALUE(joint_venture_economically) OVER w,
--            LAST_VALUE(woman_owned_business) OVER w,
--            LAST_VALUE(service_disabled_veteran_o) OVER w,
--            LAST_VALUE(veteran_owned_business) OVER w,
--            LAST_VALUE(c8a_program_participant) OVER w,
--            LAST_VALUE(the_ability_one_program) OVER w,
--            LAST_VALUE(dot_certified_disadvantage) OVER w,
--            LAST_VALUE(emerging_small_business) OVER w,
--            LAST_VALUE(federally_funded_research) OVER w,
--            LAST_VALUE(historically_underutilized) OVER w,
--            LAST_VALUE(labor_surplus_area_firm) OVER w,
--            LAST_VALUE(sba_certified_8_a_joint_ve) OVER w,
--            LAST_VALUE(self_certified_small_disad) OVER w,
--            LAST_VALUE(small_agricultural_coopera) OVER w,
--            LAST_VALUE(small_disadvantaged_busine) OVER w,
--            LAST_VALUE(community_developed_corpor) OVER w,
--            LAST_VALUE(domestic_or_foreign_entity) OVER w,
--            LAST_VALUE(foreign_owned_and_located) OVER w,
--            LAST_VALUE(foreign_government) OVER w,
--            LAST_VALUE(international_organization) OVER w,
--            LAST_VALUE(domestic_shelter) OVER w,
--            LAST_VALUE(hospital_flag) OVER w,
--            LAST_VALUE(veterinary_hospital) OVER w,
--            LAST_VALUE(foundation) OVER w,
--            LAST_VALUE(community_development_corp) OVER w,
--            LAST_VALUE(nonprofit_organization) OVER w,
--            LAST_VALUE(educational_institution) OVER w,
--            LAST_VALUE(other_not_for_profit_organ) OVER w,
--            LAST_VALUE(state_controlled_instituti) OVER w,
--            LAST_VALUE(c1862_land_grant_college) OVER w,
--            LAST_VALUE(c1890_land_grant_college) OVER w,
--            LAST_VALUE(c1994_land_grant_college) OVER w,
--            LAST_VALUE(private_university_or_coll) OVER w,
--            LAST_VALUE(minority_institution) OVER w,
--            LAST_VALUE(historically_black_college) OVER w,
--            LAST_VALUE(tribal_college) OVER w,
--            LAST_VALUE(alaskan_native_servicing_i) OVER w,
--            LAST_VALUE(native_hawaiian_servicing) OVER w,
--            LAST_VALUE(hispanic_servicing_institu) OVER w,
--            LAST_VALUE(school_of_forestry) OVER w,
--            LAST_VALUE(veterinary_college) OVER w,
--            LAST_VALUE(us_federal_government) OVER w,
--            LAST_VALUE(federal_agency) OVER w,
--            LAST_VALUE(us_government_entity) OVER w,
--            LAST_VALUE(interstate_entity) OVER w,
--            LAST_VALUE(us_state_government) OVER w,
--            LAST_VALUE(council_of_governments) OVER w,
--            LAST_VALUE(city_local_government) OVER w,
--            LAST_VALUE(county_local_government) OVER w,
--            LAST_VALUE(inter_municipal_local_gove) OVER w,
--            LAST_VALUE(municipality_local_governm) OVER w,
--            LAST_VALUE(township_local_government) OVER w,
--            LAST_VALUE(us_local_government) OVER w,
--            LAST_VALUE(local_government_owned) OVER w,
--            LAST_VALUE(school_district_local_gove) OVER w,
--            LAST_VALUE(us_tribal_government) OVER w,
--            LAST_VALUE(indian_tribe_federally_rec) OVER w,
--            LAST_VALUE(housing_authorities_public) OVER w,
--            LAST_VALUE(airport_authority) OVER w,
--            LAST_VALUE(port_authority) OVER w,
--            LAST_VALUE(transit_authority) OVER w,
--            LAST_VALUE(planning_commission) OVER w
--        ) AS business_categories,
        NULL::text AS cfda_number,
        NULL::text AS cfda_title,
        NULL::text AS sai_number,

        -- recipient data
        LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id, -- DUNS
        LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
        LAST_VALUE(ultimate_parent_unique_ide) OVER w AS parent_recipient_unique_id,

        -- executive compensation data
--        exec_comp.high_comp_officer1_full_na AS officer_1_name,
--        exec_comp.high_comp_officer1_amount AS officer_1_amount,
--        exec_comp.high_comp_officer2_full_na AS officer_2_name,
--        exec_comp.high_comp_officer2_amount AS officer_2_amount,
--        exec_comp.high_comp_officer3_full_na AS officer_3_name,
--        exec_comp.high_comp_officer3_amount AS officer_3_amount,
--        exec_comp.high_comp_officer4_full_na AS officer_4_name,
--        exec_comp.high_comp_officer4_amount AS officer_4_amount,
--        exec_comp.high_comp_officer5_full_na AS officer_5_name,
--        exec_comp.high_comp_officer5_amount AS officer_5_amount,

        -- business categories
        LAST_VALUE(legal_entity_address_line1) OVER w AS recipient_location_address_line1,
        LAST_VALUE(legal_entity_address_line2) OVER w AS recipient_location_address_line2,
        LAST_VALUE(legal_entity_address_line3) OVER w AS recipient_location_address_line3,

        -- foreign province
        NULL::text AS recipient_location_foreign_province,
        NULL::text AS recipient_location_foreign_city_name,
        NULL::text AS recipient_location_foreign_city_name,

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
        NULL::text AS recipient_location_city_code,
        LAST_VALUE(legal_entity_city_name) OVER w AS recipient_location_city_name,

        -- zip
        LAST_VALUE(legal_entity_zip5) OVER w AS recipient_location_zip5,

        -- congressional disctrict
        LAST_VALUE(legal_entity_congressional) OVER w AS recipient_location_congressional_code,

        -- ppop data
        NULL::text AS pop_code,

        -- foreign
        NULL::text AS pop_foreign_province,

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
    WINDOW w AS (partition BY tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
    ORDER BY
        tf.piid,
        tf.parent_award_id,
        tf.agency_id,
        tf.referenced_idv_agency_iden,
        tf.action_date desc,
        tf.award_modification_amendme desc,
        tf.transaction_number desc') AS fpds_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        type_of_idc text,
        type_of_idc_description text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation numeric,
        total_subsidy_cost numeric,
        total_loan_value numeric,
        total_outlay numeric,
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award numeric,
        base_and_all_options_value numeric,
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount numeric,
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        business_types_description text,
--        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
--        officer_1_name text,
--        officer_1_amount text,
--        officer_2_name text,
--        officer_2_amount text,
--        officer_3_name text,
--        officer_3_amount text,
--        officer_4_name text,
--        officer_4_amount text,
--        officer_5_name text,
--        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,

        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,

        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,

        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,

        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,

        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,

        -- congressional disctrict
        recipient_location_congressional_code text,

        -- ppop data
        pop_code text,

        -- foreign
        pop_foreign_province text,

        -- country
        pop_country_code text,
        pop_country_name text,

        -- state
        pop_state_code text,
        pop_state_name text,

        -- county
        pop_county_code text,
        pop_county_name text,

        -- city
        pop_city_name text,

        -- zip
        pop_zip5 text,

        -- congressional disctrict
        pop_congressional_code text
    )
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)

UNION ALL

(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fabs_fain_uniq_awards.agency_id AS agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    type_of_idc,
    type_of_idc_description,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_loan_value,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c AS awarding_subtier_agency_code,
    awarding_sub_tier_agency_n AS awarding_subtier_agency_name,
    awarding_agency.subtier_abbr AS awarding_subtier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co AS funding_subtier_agency_code,
    funding_sub_tier_agency_na AS funding_subtier_agency_name,
    funding_agency.subtier_abbr AS funding_subtier_agency_abbr,
    funding_office_code,
    funding_office_name,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    business_types_description,
--    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    cfda.objectives AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
--    officer_1_name,
--    officer_1_amount,
--    officer_2_name,
--    officer_2_amount,
--    officer_3_name,
--    officer_3_amount,
--    officer_4_name,
--    officer_4_amount,
--    officer_5_name,
--    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month'))::INT AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
    DISTINCT ON (pafa.fain, pafa.awarding_sub_tier_agency_c)
    ''ASST_AW_'' ||
        COALESCE(pafa.awarding_sub_tier_agency_c,''-NONE-'') || ''_'' ||
        COALESCE(pafa.fain, ''-NONE-'') || ''_'' ||
        ''-NONE-'' AS generated_unique_award_id,
    LAST_VALUE(assistance_type) OVER w AS type,
    CASE
        WHEN LAST_VALUE(assistance_type) OVER w = ''02'' THEN ''Block Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''03'' THEN ''Formula Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''04'' THEN ''Project Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''05'' THEN ''Cooperative Agreement''
        WHEN LAST_VALUE(assistance_type) OVER w = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN LAST_VALUE(assistance_type) OVER w = ''07'' THEN ''Direct Loan''
        WHEN LAST_VALUE(assistance_type) OVER w = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN LAST_VALUE(assistance_type) OVER w = ''09'' THEN ''Insurance''
        WHEN LAST_VALUE(assistance_type) OVER w = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN LAST_VALUE(assistance_type) OVER w = ''11'' THEN ''Other Financial Assistance''
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
    LAST_VALUE(fain) OVER w AS fain,
    NULL::text AS uri,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC)) OVER w AS total_obligation,
    SUM(COALESCE(pafa.original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) OVER w AS total_subsidy_cost,
    SUM(COALESCE(pafa.face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) OVER w AS total_loan_value,
    NULL::NUMERIC AS total_outlay,
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
    NULLIF(LAST_VALUE(action_date) OVER w, '''')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '''')::DATE) OVER w AS date_signed,
    LAST_VALUE(award_description) OVER w AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(NULLIF(pafa.period_of_performance_star, '''')::DATE) OVER w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '''')::DATE) OVER w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    LAST_VALUE(modified_at::DATE) OVER w AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '''')::DATE) OVER w AS certified_date,
    LAST_VALUE(record_type) OVER w AS record_type,
    LAST_VALUE(afa_generated_unique) OVER w AS latest_transaction_unique_id,
    0 AS total_subaward_amount,
    0 AS subaward_count,
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
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''A'' THEN ''State government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''B'' THEN ''County Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''C'' THEN ''City or Township Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''D'' THEN ''Special District Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''E'' THEN ''Regional Organization''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''F'' THEN ''U.S. Territory or Possession''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''G'' THEN ''Independent School District''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''H'' THEN ''Public/State Controlled Institution of Higher Education''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''I'' THEN ''Indian/Native American Tribal Government (Federally Recognized)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''J'' THEN ''Indian/Native American Tribal Government (Other than Federally Recognized)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''K'' THEN ''Indian/Native American Tribal Designated Organization''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''L'' THEN ''Public/Indian Housing Authority''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''M'' THEN ''Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''N'' THEN ''Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''O'' THEN ''Private Institution of Higher Education''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''P'' THEN ''Individual''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''Q'' THEN ''For-Profit Organization (Other than Small Business)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''R'' THEN ''Small Business''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''S'' THEN ''Hispanic-serving Institution''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''T'' THEN ''Historically Black Colleges and Universities (HBCUs)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''U'' THEN ''Tribally Controlled Colleges and Universities (TCCUs)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''V'' THEN ''Alaska Native and Native Hawaiian Serving Institutions''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''W'' THEN ''Non-domestic (non-US) Entity''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''X'' THEN ''Other''
        ELSE ''Unknown Types''
    END AS business_types_description,
--    compile_fabs_business_categories(LAST_VALUE(business_types) OVER w) AS business_categories,
    LAST_VALUE(cfda_number) OVER w AS cfda_number,
    LAST_VALUE(cfda_title) OVER w AS cfda_title,
    LAST_VALUE(sai_number) OVER w AS sai_number,

    -- recipient data
    LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id,
    LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
--    exec_comp.high_comp_officer1_full_na AS officer_1_name,
--    exec_comp.high_comp_officer1_amount AS officer_1_amount,
--    exec_comp.high_comp_officer2_full_na AS officer_2_name,
--    exec_comp.high_comp_officer2_amount AS officer_2_amount,
--    exec_comp.high_comp_officer3_full_na AS officer_3_name,
--    exec_comp.high_comp_officer3_amount AS officer_3_amount,
--    exec_comp.high_comp_officer4_full_na AS officer_4_name,
--    exec_comp.high_comp_officer4_amount AS officer_4_amount,
--    exec_comp.high_comp_officer5_full_na AS officer_5_name,
--    exec_comp.high_comp_officer5_amount AS officer_5_amount,

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
WHERE pafa.record_type = ''2'' AND is_active IS TRUE
WINDOW w AS (partition BY pafa.fain, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.fain,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc
;') AS fabs_fain_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        type_of_idc text,
        type_of_idc_description text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation numeric,
        total_subsidy_cost numeric,
        total_loan_value numeric,
        total_outlay numeric,
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award numeric,
        base_and_all_options_value numeric,
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount numeric,
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        business_types_description text,
--        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
--        officer_1_name text,
--        officer_1_amount text,
--        officer_2_name text,
--        officer_2_amount text,
--        officer_3_name text,
--        officer_3_amount text,
--        officer_4_name text,
--        officer_4_amount text,
--        officer_5_name text,
--        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,

        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,

        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,

        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,

        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,

        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,

        -- congressional disctrict
        recipient_location_congressional_code text,

        -- ppop data
        pop_code text,

        -- foreign
        pop_foreign_province text,

        -- country
        pop_country_code text,
        pop_country_name text,

        -- state
        pop_state_code text,
        pop_state_name text,

        -- county
        pop_county_code text,
        pop_county_name text,

        -- city
        pop_city_name text,

        -- zip
        pop_zip5 text,

        -- congressional disctrict
        pop_congressional_code text
    )
    INNER JOIN
    references_cfda AS cfda ON cfda.program_number = cfda_number
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)

UNION ALL

(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fabs_uri_uniq_awards.agency_id AS agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    type_of_idc,
    type_of_idc_description,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_loan_value,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c AS awarding_subtier_agency_code,
    awarding_sub_tier_agency_n AS awarding_subtier_agency_name,
    awarding_agency.subtier_abbr AS awarding_subtier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co AS funding_subtier_agency_code,
    funding_sub_tier_agency_na AS funding_subtier_agency_name,
    funding_agency.subtier_abbr AS funding_subtier_agency_abbr,
    funding_office_code,
    funding_office_name,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    business_types_description,
--    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    cfda.objectives AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
--    officer_1_name,
--    officer_1_amount,
--    officer_2_name,
--    officer_2_amount,
--    officer_3_name,
--    officer_3_amount,
--    officer_4_name,
--    officer_4_amount,
--    officer_5_name,
--    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month'))::INT AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
    DISTINCT ON (pafa.uri, pafa.awarding_sub_tier_agency_c)
    ''ASST_AW'' ||
        COALESCE(pafa.awarding_sub_tier_agency_c,''-NONE-'') || ''_'' ||
        ''-NONE-'' || ''_'' ||
        COALESCE(pafa.uri, ''-NONE-'') AS generated_unique_award_id,
    LAST_VALUE(assistance_type) OVER w AS type,
    CASE
        WHEN LAST_VALUE(assistance_type) OVER w = ''02'' THEN ''Block Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''03'' THEN ''Formula Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''04'' THEN ''Project Grant''
        WHEN LAST_VALUE(assistance_type) OVER w = ''05'' THEN ''Cooperative Agreement''
        WHEN LAST_VALUE(assistance_type) OVER w = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN LAST_VALUE(assistance_type) OVER w = ''07'' THEN ''Direct Loan''
        WHEN LAST_VALUE(assistance_type) OVER w = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN LAST_VALUE(assistance_type) OVER w = ''09'' THEN ''Insurance''
        WHEN LAST_VALUE(assistance_type) OVER w = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN LAST_VALUE(assistance_type) OVER w = ''11'' THEN ''Other Financial Assistance''
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
    NULL::NUMERIC AS total_outlay,
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
    NULLIF(LAST_VALUE(action_date) OVER w, '''')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '''')::DATE) OVER w AS date_signed,
    LAST_VALUE(award_description) OVER w AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(NULLIF(pafa.period_of_performance_star, '''')::DATE) OVER w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '''')::DATE) OVER w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    LAST_VALUE(modified_at::DATE) OVER w AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '''')::DATE) OVER w AS certified_date,
    LAST_VALUE(record_type) OVER w AS record_type,
    LAST_VALUE(afa_generated_unique) OVER w AS latest_transaction_unique_id,
    0 AS total_subaward_amount,
    0 AS subaward_count,
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
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''A'' THEN ''State government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''B'' THEN ''County Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''C'' THEN ''City or Township Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''D'' THEN ''Special District Government''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''E'' THEN ''Regional Organization''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''F'' THEN ''U.S. Territory or Possession''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''G'' THEN ''Independent School District''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''H'' THEN ''Public/State Controlled Institution of Higher Education''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''I'' THEN ''Indian/Native American Tribal Government (Federally Recognized)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''J'' THEN ''Indian/Native American Tribal Government (Other than Federally Recognized)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''K'' THEN ''Indian/Native American Tribal Designated Organization''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''L'' THEN ''Public/Indian Housing Authority''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''M'' THEN ''Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''N'' THEN ''Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''O'' THEN ''Private Institution of Higher Education''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''P'' THEN ''Individual''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''Q'' THEN ''For-Profit Organization (Other than Small Business)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''R'' THEN ''Small Business''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''S'' THEN ''Hispanic-serving Institution''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''T'' THEN ''Historically Black Colleges and Universities (HBCUs)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''U'' THEN ''Tribally Controlled Colleges and Universities (TCCUs)''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''V'' THEN ''Alaska Native and Native Hawaiian Serving Institutions''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''W'' THEN ''Non-domestic (non-US) Entity''
        WHEN UPPER(LAST_VALUE(business_types) OVER w) = ''X'' THEN ''Other''
        ELSE ''Unknown Types''
    END AS business_types_description,
--    compile_fabs_business_categories(LAST_VALUE(business_types) OVER w) AS business_categories,
    LAST_VALUE(cfda_number) OVER w AS cfda_number,
    LAST_VALUE(cfda_title) OVER w AS cfda_title,
    LAST_VALUE(sai_number) OVER w AS sai_number,

    -- recipient data
    LAST_VALUE(awardee_or_recipient_uniqu) OVER w AS recipient_unique_id,
    LAST_VALUE(awardee_or_recipient_legal) OVER w AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
--    exec_comp.high_comp_officer1_full_na AS officer_1_name,
--    exec_comp.high_comp_officer1_amount AS officer_1_amount,
--    exec_comp.high_comp_officer2_full_na AS officer_2_name,
--    exec_comp.high_comp_officer2_amount AS officer_2_amount,
--    exec_comp.high_comp_officer3_full_na AS officer_3_name,
--    exec_comp.high_comp_officer3_amount AS officer_3_amount,
--    exec_comp.high_comp_officer4_full_na AS officer_4_name,
--    exec_comp.high_comp_officer4_amount AS officer_4_amount,
--    exec_comp.high_comp_officer5_full_na AS officer_5_name,
--    exec_comp.high_comp_officer5_amount AS officer_5_amount,

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
WHERE pafa.record_type = ''1'' AND is_active IS TRUE
WINDOW w AS (partition BY pafa.uri, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.uri,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc') AS fabs_uri_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        type_of_idc text,
        type_of_idc_description text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation numeric,
        total_subsidy_cost numeric,
        total_loan_value numeric,
        total_outlay numeric,
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award numeric,
        base_and_all_options_value numeric,
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount numeric,
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        business_types_description text,
--        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
--        officer_1_name text,
--        officer_1_amount text,
--        officer_2_name text,
--        officer_2_amount text,
--        officer_3_name text,
--        officer_3_amount text,
--        officer_4_name text,
--        officer_4_amount text,
--        officer_5_name text,
--        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,

        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,

        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,

        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,

        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,

        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,

        -- congressional disctrict
        recipient_location_congressional_code text,

        -- ppop data
        pop_code text,

        -- foreign
        pop_foreign_province text,

        -- country
        pop_country_code text,
        pop_country_name text,

        -- state
        pop_state_code text,
        pop_state_name text,

        -- county
        pop_county_code text,
        pop_county_name text,

        -- city
        pop_city_name text,

        -- zip
        pop_zip5 text,

        -- congressional disctrict
        pop_congressional_code text
    )
    INNER JOIN
    references_cfda AS cfda ON cfda.program_number = cfda_number
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)
);

BEGIN;
ALTER MATERIALIZED VIEW IF EXISTS award_matview RENAME TO award_matview_old;
ALTER MATERIALIZED VIEW award_matview_new RENAME TO award_matview;
DROP MATERIALIZED VIEW IF EXISTS award_matview_old;
COMMIT;