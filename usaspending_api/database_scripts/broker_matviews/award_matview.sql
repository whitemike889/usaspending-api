DROP MATERIALIZED VIEW IF EXISTS award_matview_new;

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
    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    NULL::text AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

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
        latest_transaction.contract_award_type AS type,
        latest_transaction.contract_award_type_desc AS type_description,
        latest_transaction.agency_id AS agency_id,
        latest_transaction.referenced_idv_agency_iden AS referenced_idv_agency_iden,
        latest_transaction.referenced_idv_agency_desc AS referenced_idv_agency_desc,
        latest_transaction.multiple_or_single_award_i AS multiple_or_single_award_i,
        latest_transaction.multiple_or_single_aw_desc AS multiple_or_single_aw_desc,
        latest_transaction.type_of_idc AS type_of_idc,
        latest_transaction.type_of_idc_description AS type_of_idc_description,
        latest_transaction.piid AS piid,
        latest_transaction.parent_award_id AS parent_award_piid,
        NULL::text AS fain,
        NULL::text AS uri,
        SUM(COALESCE(tf.federal_action_obligation::NUMERIC, 0::NUMERIC)) over w AS total_obligation,
        NULL::NUMERIC AS total_subsidy_cost,
        NULL::NUMERIC AS total_loan_value,
        NULL::NUMERIC AS total_outlay,
        latest_transaction.awarding_agency_code AS awarding_agency_code,
        latest_transaction.awarding_agency_name AS awarding_agency_name,
        latest_transaction.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        latest_transaction.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        latest_transaction.awarding_office_code AS awarding_office_code,
        latest_transaction.awarding_office_name AS awarding_office_name,
        latest_transaction.funding_agency_code AS funding_agency_code,
        latest_transaction.funding_agency_name AS funding_agency_name,
        latest_transaction.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        latest_transaction.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        latest_transaction.funding_office_code AS funding_office_code,
        latest_transaction.funding_office_name AS funding_office_name,
        NULLIF(latest_transaction.action_date, '''')::DATE AS action_date,
        MIN(NULLIF(tf.action_date, '''')::DATE) over w AS date_signed,
        latest_transaction.award_description AS description,
        -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
        MIN(NULLIF(tf.period_of_performance_star, '''')::DATE) over w AS period_of_performance_start_date,
        MAX(NULLIF(tf.period_of_performance_curr, '''')::DATE) over w AS period_of_performance_current_end_date,
        NULL::NUMERIC AS potential_total_value_of_award,
        SUM(COALESCE(tf.base_and_all_options_value::NUMERIC, 0::NUMERIC)) over w AS base_and_all_options_value,
        latest_transaction.last_modified::DATE AS last_modified_date,
        MAX(NULLIF(tf.action_date, '''')::DATE) over w AS certified_date,
        NULL::int AS record_type,
        latest_transaction.detached_award_proc_unique AS latest_transaction_unique_id,
        0 AS total_subaward_amount,
        0 AS subaward_count,
        latest_transaction.pulled_from AS pulled_from,
        latest_transaction.product_or_service_code AS product_or_service_code,
        latest_transaction.product_or_service_co_desc AS product_or_service_co_desc,
        latest_transaction.extent_competed AS extent_competed,
        latest_transaction.extent_compete_description AS extent_compete_description,
        latest_transaction.type_of_contract_pricing AS type_of_contract_pricing,
        latest_transaction.type_of_contract_pric_desc AS type_of_contract_pric_desc,
        latest_transaction.contract_award_type_desc AS contract_award_type_desc,
        latest_transaction.cost_or_pricing_data AS cost_or_pricing_data,
        latest_transaction.cost_or_pricing_data_desc AS cost_or_pricing_data_desc,
        latest_transaction.domestic_or_foreign_entity AS domestic_or_foreign_entity,
        latest_transaction.domestic_or_foreign_e_desc AS domestic_or_foreign_e_desc,
        latest_transaction.fair_opportunity_limited_s AS fair_opportunity_limited_s,
        latest_transaction.fair_opportunity_limi_desc AS fair_opportunity_limi_desc,
        latest_transaction.foreign_funding AS foreign_funding,
        latest_transaction.foreign_funding_desc AS foreign_funding_desc,
        latest_transaction.interagency_contracting_au AS interagency_contracting_au,
        latest_transaction.interagency_contract_desc AS interagency_contract_desc,
        latest_transaction.major_program AS major_program,
        latest_transaction.price_evaluation_adjustmen AS price_evaluation_adjustmen,
        latest_transaction.program_acronym AS program_acronym,
        latest_transaction.subcontracting_plan AS subcontracting_plan,
        latest_transaction.subcontracting_plan_desc AS subcontracting_plan_desc,
        latest_transaction.multi_year_contract AS multi_year_contract,
        latest_transaction.multi_year_contract_desc AS multi_year_contract_desc,
        latest_transaction.purchase_card_as_payment_m AS purchase_card_as_payment_m,
        latest_transaction.purchase_card_as_paym_desc AS purchase_card_as_paym_desc,
        latest_transaction.consolidated_contract AS consolidated_contract,
        latest_transaction.consolidated_contract_desc AS consolidated_contract_desc,
        latest_transaction.solicitation_identifier AS solicitation_identifier,
        latest_transaction.solicitation_procedures AS solicitation_procedures,
        latest_transaction.solicitation_procedur_desc AS solicitation_procedur_desc,
        latest_transaction.number_of_offers_received AS number_of_offers_received,
        latest_transaction.other_than_full_and_open_c AS other_than_full_and_open_c,
        latest_transaction.other_than_full_and_o_desc AS other_than_full_and_o_desc,
        latest_transaction.commercial_item_acquisitio AS commercial_item_acquisitio,
        latest_transaction.commercial_item_acqui_desc AS commercial_item_acqui_desc,
        latest_transaction.commercial_item_test_progr AS commercial_item_test_progr,
        latest_transaction.commercial_item_test_desc AS commercial_item_test_desc,
        latest_transaction.evaluated_preference AS evaluated_preference,
        latest_transaction.evaluated_preference_desc AS evaluated_preference_desc,
        latest_transaction.fed_biz_opps AS fed_biz_opps,
        latest_transaction.fed_biz_opps_description AS fed_biz_opps_description,
        latest_transaction.small_business_competitive AS small_business_competitive,
        latest_transaction.dod_claimant_program_code AS dod_claimant_program_code,
        latest_transaction.dod_claimant_prog_cod_desc AS dod_claimant_prog_cod_desc,
        latest_transaction.program_system_or_equipmen AS program_system_or_equipmen,
        latest_transaction.program_system_or_equ_desc AS program_system_or_equ_desc,
        latest_transaction.information_technology_com AS information_technology_com,
        latest_transaction.information_technolog_desc AS information_technolog_desc,
        latest_transaction.sea_transportation AS sea_transportation,
        latest_transaction.sea_transportation_desc AS sea_transportation_desc,
        latest_transaction.clinger_cohen_act_planning AS clinger_cohen_act_planning,
        latest_transaction.clinger_cohen_act_pla_desc AS clinger_cohen_act_pla_desc,
        latest_transaction.davis_bacon_act AS davis_bacon_act,
        latest_transaction.davis_bacon_act_descrip AS davis_bacon_act_descrip,
        latest_transaction.service_contract_act AS service_contract_act,
        latest_transaction.service_contract_act_desc AS service_contract_act_desc,
        latest_transaction.walsh_healey_act AS walsh_healey_act,
        latest_transaction.walsh_healey_act_descrip AS walsh_healey_act_descrip,
        latest_transaction.naics AS naics,
        latest_transaction.naics_description AS naics_description,
        latest_transaction.idv_type AS idv_type,
        latest_transaction.idv_type_description AS idv_type_description,
        latest_transaction.type_set_aside AS type_set_aside,
        latest_transaction.type_set_aside_description AS type_set_aside_description,
        NULL::text AS assistance_type,
        NULL::text AS business_funds_indicator,
        NULL::text AS business_types,
        NULL::text AS business_types_description,
        compile_fpds_business_categories(
            latest_transaction.contracting_officers_deter,
            latest_transaction.corporate_entity_tax_exemp,
            latest_transaction.corporate_entity_not_tax_e,
            latest_transaction.partnership_or_limited_lia,
            latest_transaction.sole_proprietorship,
            latest_transaction.manufacturer_of_goods,
            latest_transaction.subchapter_s_corporation,
            latest_transaction.limited_liability_corporat,
            latest_transaction.for_profit_organization,
            latest_transaction.alaskan_native_owned_corpo,
            latest_transaction.american_indian_owned_busi,
            latest_transaction.asian_pacific_american_own,
            latest_transaction.black_american_owned_busin,
            latest_transaction.hispanic_american_owned_bu,
            latest_transaction.native_american_owned_busi,
            latest_transaction.native_hawaiian_owned_busi,
            latest_transaction.subcontinent_asian_asian_i,
            latest_transaction.tribally_owned_business,
            latest_transaction.other_minority_owned_busin,
            latest_transaction.minority_owned_business,
            latest_transaction.women_owned_small_business,
            latest_transaction.economically_disadvantaged,
            latest_transaction.joint_venture_women_owned,
            latest_transaction.joint_venture_economically,
            latest_transaction.woman_owned_business,
            latest_transaction.service_disabled_veteran_o,
            latest_transaction.veteran_owned_business,
            latest_transaction.c8a_program_participant,
            latest_transaction.the_ability_one_program,
            latest_transaction.dot_certified_disadvantage,
            latest_transaction.emerging_small_business,
            latest_transaction.federally_funded_research,
            latest_transaction.historically_underutilized,
            latest_transaction.labor_surplus_area_firm,
            latest_transaction.sba_certified_8_a_joint_ve,
            latest_transaction.self_certified_small_disad,
            latest_transaction.small_agricultural_coopera,
            latest_transaction.small_disadvantaged_busine,
            latest_transaction.community_developed_corpor,
            latest_transaction.domestic_or_foreign_entity,
            latest_transaction.foreign_owned_and_located,
            latest_transaction.foreign_government,
            latest_transaction.international_organization,
            latest_transaction.domestic_shelter,
            latest_transaction.hospital_flag,
            latest_transaction.veterinary_hospital,
            latest_transaction.foundation,
            latest_transaction.community_development_corp,
            latest_transaction.nonprofit_organization,
            latest_transaction.educational_institution,
            latest_transaction.other_not_for_profit_organ,
            latest_transaction.state_controlled_instituti,
            latest_transaction.c1862_land_grant_college,
            latest_transaction.c1890_land_grant_college,
            latest_transaction.c1994_land_grant_college,
            latest_transaction.private_university_or_coll,
            latest_transaction.minority_institution,
            latest_transaction.historically_black_college,
            latest_transaction.tribal_college,
            latest_transaction.alaskan_native_servicing_i,
            latest_transaction.native_hawaiian_servicing,
            latest_transaction.hispanic_servicing_institu,
            latest_transaction.school_of_forestry,
            latest_transaction.veterinary_college,
            latest_transaction.us_federal_government,
            latest_transaction.federal_agency,
            latest_transaction.us_government_entity,
            latest_transaction.interstate_entity,
            latest_transaction.us_state_government,
            latest_transaction.council_of_governments,
            latest_transaction.city_local_government,
            latest_transaction.county_local_government,
            latest_transaction.inter_municipal_local_gove,
            latest_transaction.municipality_local_governm,
            latest_transaction.township_local_government,
            latest_transaction.us_local_government,
            latest_transaction.local_government_owned,
            latest_transaction.school_district_local_gove,
            latest_transaction.us_tribal_government,
            latest_transaction.indian_tribe_federally_rec,
            latest_transaction.housing_authorities_public,
            latest_transaction.airport_authority,
            latest_transaction.port_authority,
            latest_transaction.transit_authority,
            latest_transaction.planning_commission
        ) AS business_categories,
        NULL::text AS cfda_number,
        NULL::text AS cfda_title,
        NULL::text AS sai_number,

        -- recipient data
        latest_transaction.awardee_or_recipient_uniqu AS recipient_unique_id, -- DUNS
        latest_transaction.awardee_or_recipient_legal AS recipient_name,
        latest_transaction.ultimate_parent_unique_ide AS parent_recipient_unique_id,

        -- executive compensation data
        exec_comp.high_comp_officer1_full_na AS officer_1_name,
        exec_comp.high_comp_officer1_amount AS officer_1_amount,
        exec_comp.high_comp_officer2_full_na AS officer_2_name,
        exec_comp.high_comp_officer2_amount AS officer_2_amount,
        exec_comp.high_comp_officer3_full_na AS officer_3_name,
        exec_comp.high_comp_officer3_amount AS officer_3_amount,
        exec_comp.high_comp_officer4_full_na AS officer_4_name,
        exec_comp.high_comp_officer4_amount AS officer_4_amount,
        exec_comp.high_comp_officer5_full_na AS officer_5_name,
        exec_comp.high_comp_officer5_amount AS officer_5_amount,

        -- business categories
        latest_transaction.legal_entity_address_line1 AS recipient_location_address_line1,
        latest_transaction.legal_entity_address_line2 AS recipient_location_address_line2,
        latest_transaction.legal_entity_address_line3 AS recipient_location_address_line3,

        -- foreign province
        NULL::text AS recipient_location_foreign_province,
        NULL::text AS recipient_location_foreign_city_name,
        NULL::text AS recipient_location_foreign_city_name,

        -- country
        latest_transaction.legal_entity_country_code AS recipient_location_country_code,
        latest_transaction.legal_entity_country_name AS recipient_location_country_name,

        -- state
        latest_transaction.legal_entity_state_code AS recipient_location_state_code,
        latest_transaction.legal_entity_state_descrip AS recipient_location_state_name,

        -- county
        latest_transaction.legal_entity_county_code AS recipient_location_county_code,
        latest_transaction.legal_entity_county_name AS recipient_location_county_name,

        -- city
        NULL::text AS recipient_location_city_code,
        latest_transaction.legal_entity_city_name AS recipient_location_city_name,

        -- zip
        latest_transaction.legal_entity_zip5 AS recipient_location_zip5,

        -- congressional disctrict
        latest_transaction.legal_entity_congressional AS recipient_location_congressional_code,

        -- ppop data
        NULL::text AS pop_code,

        -- foreign
        NULL::text AS pop_foreign_province,

        -- country
        latest_transaction.place_of_perform_country_c AS pop_country_code,
        latest_transaction.place_of_perf_country_desc AS pop_country_name,

        -- state
        latest_transaction.place_of_performance_state AS pop_state_code,
        latest_transaction.place_of_perfor_state_desc AS pop_state_name,

        -- county
        latest_transaction.place_of_perform_county_co AS pop_county_code,
        latest_transaction.place_of_perform_county_na AS pop_county_name,

        -- city
        latest_transaction.place_of_perform_city_name AS pop_city_name,

        -- zip
        latest_transaction.place_of_performance_zip5 AS pop_zip5,

        -- congressional disctrict
        latest_transaction.place_of_performance_congr AS pop_congressional_code
    FROM
        detached_award_procurement tf
        INNER JOIN
        (SELECT * FROM detached_award_procurement AS tf_sub WHERE tf_sub.action_date = tf.certified_date AND tf_sub.piid = tf.piid AND tf_sub.parent_award_id = tf.parent_award_id AND tf_sub.agency_id = tf.agency_id AND tf_sub.referenced_idv_agency_iden = tf.referenced_idv_agency_iden) AS latest_transaction ON latest_transaction.detached_award_proc_unique = tf.detached_award_proc_unique
        LEFT OUTER JOIN
        exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = latest_transaction.awardee_or_recipient_uniqu
    window w AS (partition BY tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
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
        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

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
    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    cfda.objectives AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

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
    latest_transaction.assistance_type AS type,
    CASE
        WHEN latest_transaction.assistance_type = ''02'' THEN ''Block Grant''
        WHEN latest_transaction.assistance_type = ''03'' THEN ''Formula Grant''
        WHEN latest_transaction.assistance_type = ''04'' THEN ''Project Grant''
        WHEN latest_transaction.assistance_type = ''05'' THEN ''Cooperative Agreement''
        WHEN latest_transaction.assistance_type = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN latest_transaction.assistance_type = ''07'' THEN ''Direct Loan''
        WHEN latest_transaction.assistance_type = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN latest_transaction.assistance_type = ''09'' THEN ''Insurance''
        WHEN latest_transaction.assistance_type = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN latest_transaction.assistance_type = ''11'' THEN ''Other Financial Assistance''
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
    latest_transaction.fain AS fain,
    NULL::text AS uri,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC)) over w AS total_obligation,
    SUM(COALESCE(pafa.original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) over w AS total_subsidy_cost,
    SUM(COALESCE(pafa.face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) over w AS total_loan_value,
    NULL::NUMERIC AS total_outlay,
    latest_transaction.awarding_agency_code AS awarding_agency_code,
    latest_transaction.awarding_agency_name AS awarding_agency_name,
    latest_transaction.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
    latest_transaction.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
    latest_transaction.awarding_office_code AS awarding_office_code,
    latest_transaction.awarding_office_name AS awarding_office_name,
    latest_transaction.funding_agency_code AS funding_agency_code,
    latest_transaction.funding_agency_name AS funding_agency_name,
    latest_transaction.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    latest_transaction.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    latest_transaction.funding_office_code AS funding_office_code,
    latest_transaction.funding_office_name AS funding_office_name,
    NULLIF(latest_transaction.action_date, '''')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '''')::DATE) over w AS date_signed,
    latest_transaction.award_description AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(NULLIF(pafa.period_of_performance_star, '''')::DATE) over w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '''')::DATE) over w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    latest_transaction.modified_at::DATE AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '''')::DATE) over w AS certified_date,
    latest_transaction.record_type AS record_type,
    latest_transaction.afa_generated_unique AS latest_transaction_unique_id,
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
    latest_transaction.assistance_type AS assistance_type,
    latest_transaction.business_funds_indicator AS business_funds_indicator,
    latest_transaction.business_types AS business_types,
    CASE
        WHEN UPPER(latest_transaction.business_types) = ''A'' THEN ''State government''
        WHEN UPPER(latest_transaction.business_types) = ''B'' THEN ''County Government''
        WHEN UPPER(latest_transaction.business_types) = ''C'' THEN ''City or Township Government''
        WHEN UPPER(latest_transaction.business_types) = ''D'' THEN ''Special District Government''
        WHEN UPPER(latest_transaction.business_types) = ''E'' THEN ''Regional Organization''
        WHEN UPPER(latest_transaction.business_types) = ''F'' THEN ''U.S. Territory or Possession''
        WHEN UPPER(latest_transaction.business_types) = ''G'' THEN ''Independent School District''
        WHEN UPPER(latest_transaction.business_types) = ''H'' THEN ''Public/State Controlled Institution of Higher Education''
        WHEN UPPER(latest_transaction.business_types) = ''I'' THEN ''Indian/Native American Tribal Government (Federally Recognized)''
        WHEN UPPER(latest_transaction.business_types) = ''J'' THEN ''Indian/Native American Tribal Government (Other than Federally Recognized)''
        WHEN UPPER(latest_transaction.business_types) = ''K'' THEN ''Indian/Native American Tribal Designated Organization''
        WHEN UPPER(latest_transaction.business_types) = ''L'' THEN ''Public/Indian Housing Authority''
        WHEN UPPER(latest_transaction.business_types) = ''M'' THEN ''Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(latest_transaction.business_types) = ''N'' THEN ''Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(latest_transaction.business_types) = ''O'' THEN ''Private Institution of Higher Education''
        WHEN UPPER(latest_transaction.business_types) = ''P'' THEN ''Individual''
        WHEN UPPER(latest_transaction.business_types) = ''Q'' THEN ''For-Profit Organization (Other than Small Business)''
        WHEN UPPER(latest_transaction.business_types) = ''R'' THEN ''Small Business''
        WHEN UPPER(latest_transaction.business_types) = ''S'' THEN ''Hispanic-serving Institution''
        WHEN UPPER(latest_transaction.business_types) = ''T'' THEN ''Historically Black Colleges and Universities (HBCUs)''
        WHEN UPPER(latest_transaction.business_types) = ''U'' THEN ''Tribally Controlled Colleges and Universities (TCCUs)''
        WHEN UPPER(latest_transaction.business_types) = ''V'' THEN ''Alaska Native and Native Hawaiian Serving Institutions''
        WHEN UPPER(latest_transaction.business_types) = ''W'' THEN ''Non-domestic (non-US) Entity''
        WHEN UPPER(latest_transaction.business_types) = ''X'' THEN ''Other''
        ELSE ''Unknown Types''
    END AS business_types_description,
    compile_fabs_business_categories(latest_transaction.business_types) AS business_categories,
    latest_transaction.cfda_number AS cfda_number,
    latest_transaction.cfda_title AS cfda_title,
    latest_transaction.sai_number AS sai_number,

    -- recipient data
    latest_transaction.awardee_or_recipient_uniqu AS recipient_unique_id,
    latest_transaction.awardee_or_recipient_legal AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na AS officer_1_name,
    exec_comp.high_comp_officer1_amount AS officer_1_amount,
    exec_comp.high_comp_officer2_full_na AS officer_2_name,
    exec_comp.high_comp_officer2_amount AS officer_2_amount,
    exec_comp.high_comp_officer3_full_na AS officer_3_name,
    exec_comp.high_comp_officer3_amount AS officer_3_amount,
    exec_comp.high_comp_officer4_full_na AS officer_4_name,
    exec_comp.high_comp_officer4_amount AS officer_4_amount,
    exec_comp.high_comp_officer5_full_na AS officer_5_name,
    exec_comp.high_comp_officer5_amount AS officer_5_amount,

    -- business categories
    latest_transaction.legal_entity_address_line1 AS recipient_location_address_line1,
    latest_transaction.legal_entity_address_line2 AS recipient_location_address_line2,
    latest_transaction.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    latest_transaction.legal_entity_foreign_provi AS recipient_location_foreign_province,
    latest_transaction.legal_entity_foreign_city AS recipient_location_foreign_city_name,
    latest_transaction.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

    -- country
    latest_transaction.legal_entity_country_code AS recipient_location_country_code,
    latest_transaction.legal_entity_country_name AS recipient_location_country_name,

    -- state
    latest_transaction.legal_entity_state_code AS recipient_location_state_code,
    latest_transaction.legal_entity_state_name AS recipient_location_state_name,

    -- county
    latest_transaction.legal_entity_county_code AS recipient_location_county_code,
    latest_transaction.legal_entity_county_name AS recipient_location_county_name,

    -- city
    latest_transaction.legal_entity_city_code AS recipient_location_city_code,
    latest_transaction.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    latest_transaction.legal_entity_zip5 AS recipient_location_zip5,

    -- congressional disctrict
    latest_transaction.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    latest_transaction.place_of_performance_code AS pop_code,

    -- foreign
    latest_transaction.place_of_performance_forei AS pop_foreign_province,

    -- country
    latest_transaction.place_of_perform_country_c AS pop_country_code,
    latest_transaction.place_of_perform_country_n AS pop_country_name,

    -- state
    latest_transaction.place_of_perfor_state_code AS pop_state_code,
    latest_transaction.place_of_perform_state_nam AS pop_state_name,

    -- county
    latest_transaction.place_of_perform_county_co AS pop_county_code,
    latest_transaction.place_of_perform_county_na AS pop_county_name,

    -- city
    latest_transaction.place_of_performance_city AS pop_city_name,

    -- zip
    latest_transaction.place_of_performance_zip5 AS pop_zip5,

    -- congressional disctrict
    latest_transaction.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
    INNER JOIN
    (SELECT * FROM published_award_financial_assistance AS pafa_sub WHERE pafa_sub.action_date = pafa.certified_date AND pafa_sub.fain = pafa.fain AND pafa_sub.awarding_sub_tier_agency_c = pafa.awarding_sub_tier_agency_c) AS latest_transaction ON latest_transaction.afa_generated_unique = pafa.afa_generated_unique
    LEFT OUTER JOIN
    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = latest_transaction.awardee_or_recipient_uniqu
WHERE pafa.record_type = ''2'' AND is_active=TRUE
window w AS (partition BY pafa.fain, pafa.awarding_sub_tier_agency_c)
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
        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

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
    business_categories,
    cfda_number,
    cfda_title,
    sai_number,
    cfda.objectives AS cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

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
    latest_transaction.assistance_type AS type,
    CASE
        WHEN latest_transaction.assistance_type = ''02'' THEN ''Block Grant''
        WHEN latest_transaction.assistance_type = ''03'' THEN ''Formula Grant''
        WHEN latest_transaction.assistance_type = ''04'' THEN ''Project Grant''
        WHEN latest_transaction.assistance_type = ''05'' THEN ''Cooperative Agreement''
        WHEN latest_transaction.assistance_type = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN latest_transaction.assistance_type = ''07'' THEN ''Direct Loan''
        WHEN latest_transaction.assistance_type = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN latest_transaction.assistance_type = ''09'' THEN ''Insurance''
        WHEN latest_transaction.assistance_type = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN latest_transaction.assistance_type = ''11'' THEN ''Other Financial Assistance''
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
    latest_transaction.uri AS uri,
    SUM(COALESCE(pafa.federal_action_obligation::NUMERIC, 0::NUMERIC)) over w AS total_obligation,
    SUM(COALESCE(pafa.original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) over w AS total_subsidy_cost,
    SUM(COALESCE(pafa.face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) over w AS total_loan_value,
    NULL::NUMERIC AS total_outlay,
    latest_transaction.awarding_agency_code AS awarding_agency_code,
    latest_transaction.awarding_agency_name AS awarding_agency_name,
    latest_transaction.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
    latest_transaction.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
    latest_transaction.awarding_office_code AS awarding_office_code,
    latest_transaction.awarding_office_name AS awarding_office_name,
    latest_transaction.funding_agency_code AS funding_agency_code,
    latest_transaction.funding_agency_name AS funding_agency_name,
    latest_transaction.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    latest_transaction.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    latest_transaction.funding_office_code AS funding_office_code,
    latest_transaction.funding_office_name AS funding_office_name,
    NULLIF(latest_transaction.action_date, '''')::DATE AS action_date,
    MIN(NULLIF(pafa.action_date, '''')::DATE) over w AS date_signed,
    latest_transaction.award_description AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(NULLIF(pafa.period_of_performance_star, '''')::DATE) over w AS period_of_performance_start_date,
    MAX(NULLIF(pafa.period_of_performance_curr, '''')::DATE) over w AS period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    NULL::NUMERIC AS base_and_all_options_value,
    latest_transaction.modified_at::DATE AS last_modified_date,
    MAX(NULLIF(pafa.action_date, '''')::DATE) over w AS certified_date,
    latest_transaction.record_type AS record_type,
    latest_transaction.afa_generated_unique AS latest_transaction_unique_id,
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
    latest_transaction.assistance_type AS assistance_type,
    latest_transaction.business_funds_indicator AS business_funds_indicator,
    latest_transaction.business_types AS business_types,
    CASE
        WHEN UPPER(latest_transaction.business_types) = ''A'' THEN ''State government''
        WHEN UPPER(latest_transaction.business_types) = ''B'' THEN ''County Government''
        WHEN UPPER(latest_transaction.business_types) = ''C'' THEN ''City or Township Government''
        WHEN UPPER(latest_transaction.business_types) = ''D'' THEN ''Special District Government''
        WHEN UPPER(latest_transaction.business_types) = ''E'' THEN ''Regional Organization''
        WHEN UPPER(latest_transaction.business_types) = ''F'' THEN ''U.S. Territory or Possession''
        WHEN UPPER(latest_transaction.business_types) = ''G'' THEN ''Independent School District''
        WHEN UPPER(latest_transaction.business_types) = ''H'' THEN ''Public/State Controlled Institution of Higher Education''
        WHEN UPPER(latest_transaction.business_types) = ''I'' THEN ''Indian/Native American Tribal Government (Federally Recognized)''
        WHEN UPPER(latest_transaction.business_types) = ''J'' THEN ''Indian/Native American Tribal Government (Other than Federally Recognized)''
        WHEN UPPER(latest_transaction.business_types) = ''K'' THEN ''Indian/Native American Tribal Designated Organization''
        WHEN UPPER(latest_transaction.business_types) = ''L'' THEN ''Public/Indian Housing Authority''
        WHEN UPPER(latest_transaction.business_types) = ''M'' THEN ''Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(latest_transaction.business_types) = ''N'' THEN ''Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)''
        WHEN UPPER(latest_transaction.business_types) = ''O'' THEN ''Private Institution of Higher Education''
        WHEN UPPER(latest_transaction.business_types) = ''P'' THEN ''Individual''
        WHEN UPPER(latest_transaction.business_types) = ''Q'' THEN ''For-Profit Organization (Other than Small Business)''
        WHEN UPPER(latest_transaction.business_types) = ''R'' THEN ''Small Business''
        WHEN UPPER(latest_transaction.business_types) = ''S'' THEN ''Hispanic-serving Institution''
        WHEN UPPER(latest_transaction.business_types) = ''T'' THEN ''Historically Black Colleges and Universities (HBCUs)''
        WHEN UPPER(latest_transaction.business_types) = ''U'' THEN ''Tribally Controlled Colleges and Universities (TCCUs)''
        WHEN UPPER(latest_transaction.business_types) = ''V'' THEN ''Alaska Native and Native Hawaiian Serving Institutions''
        WHEN UPPER(latest_transaction.business_types) = ''W'' THEN ''Non-domestic (non-US) Entity''
        WHEN UPPER(latest_transaction.business_types) = ''X'' THEN ''Other''
        ELSE ''Unknown Types''
    END AS business_types_description,
    compile_fabs_business_categories(latest_transaction.business_types) AS business_categories,
    latest_transaction.cfda_number AS cfda_number,
    latest_transaction.cfda_title AS cfda_title,
    latest_transaction.sai_number AS sai_number,

    -- recipient data
    latest_transaction.awardee_or_recipient_uniqu AS recipient_unique_id,
    latest_transaction.awardee_or_recipient_legal AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na AS officer_1_name,
    exec_comp.high_comp_officer1_amount AS officer_1_amount,
    exec_comp.high_comp_officer2_full_na AS officer_2_name,
    exec_comp.high_comp_officer2_amount AS officer_2_amount,
    exec_comp.high_comp_officer3_full_na AS officer_3_name,
    exec_comp.high_comp_officer3_amount AS officer_3_amount,
    exec_comp.high_comp_officer4_full_na AS officer_4_name,
    exec_comp.high_comp_officer4_amount AS officer_4_amount,
    exec_comp.high_comp_officer5_full_na AS officer_5_name,
    exec_comp.high_comp_officer5_amount AS officer_5_amount,

    -- business categories
    latest_transaction.legal_entity_address_line1 AS recipient_location_address_line1,
    latest_transaction.legal_entity_address_line2 AS recipient_location_address_line2,
    latest_transaction.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    latest_transaction.legal_entity_foreign_provi AS recipient_location_foreign_province,
    latest_transaction.legal_entity_foreign_city AS recipient_location_foreign_city_name,
    latest_transaction.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

    -- country
    latest_transaction.legal_entity_country_code AS recipient_location_country_code,
    latest_transaction.legal_entity_country_name AS recipient_location_country_name,

    -- state
    latest_transaction.legal_entity_state_code AS recipient_location_state_code,
    latest_transaction.legal_entity_state_name AS recipient_location_state_name,

    -- county
    latest_transaction.legal_entity_county_code AS recipient_location_county_code,
    latest_transaction.legal_entity_county_name AS recipient_location_county_name,

    -- city
    latest_transaction.legal_entity_city_code AS recipient_location_city_code,
    latest_transaction.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    latest_transaction.legal_entity_zip5 AS recipient_location_zip5,

    -- congressional disctrict
    latest_transaction.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    latest_transaction.place_of_performance_code AS pop_code,

    -- foreign
    place_of_performance_forei AS pop_foreign_province,

    -- country
    latest_transaction.place_of_perform_country_c AS pop_country_code,
    latest_transaction.place_of_perform_country_n AS pop_country_name,

    -- state
    latest_transaction.place_of_perfor_state_code AS pop_state_code,
    latest_transaction.place_of_perform_state_nam AS pop_state_name,

    -- county
    latest_transaction.place_of_perform_county_co AS pop_county_code,
    latest_transaction.place_of_perform_county_na AS pop_county_name,

    -- city
    latest_transaction.place_of_performance_city AS pop_city_name,

    -- zip
    latest_transaction.place_of_performance_zip5 AS pop_zip5,

    -- congressional disctrict
    latest_transaction.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
    INNER JOIN
    (SELECT * FROM published_award_financial_assistance AS pafa_sub WHERE pafa_sub.action_date = pafa.certified_date AND pafa_sub.uri = pafa.uri AND pafa_sub.awarding_sub_tier_agency_c = pafa.awarding_sub_tier_agency_c) AS latest_transaction ON latest_transaction.afa_generated_unique = pafa.afa_generated_unique
    LEFT OUTER JOIN
    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = latest_transaction.awardee_or_recipient_uniqu
WHERE pafa.record_type = ''1'' AND is_active=TRUE
window w AS (partition BY pafa.uri, pafa.awarding_sub_tier_agency_c)
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
        business_categories text[],
        cfda_number text,
        cfda_title text,
        sai_number text,
        
        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

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
ALTER MATERIALIZED VIEW IF EXISTS award_matview_new RENAME TO award_matview;
DROP MATERIALIZED VIEW IF EXISTS award_matview_old;
COMMIT;