import ast
import logging

from collections import OrderedDict
from datetime import date
from fiscalyear import FiscalDate
from functools import total_ordering

from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import ExtractMonth, Cast, Coalesce

from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.v2.filters.view_selector import can_use_view
from usaspending_api.awards.v2.filters.view_selector import get_view_queryset
from usaspending_api.awards.v2.filters.view_selector import spending_by_award_count
from usaspending_api.awards.v2.filters.view_selector import spending_by_geography
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.awards.v2.filters.view_selector import award_spending_summary
from usaspending_api.awards.v2.filters.view_selector import transaction_spending_summary
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.awards.v2.lookups.lookups import non_loan_assistance_type_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import award_contracts_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import loan_award_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import non_loan_assistance_award_mapping
from usaspending_api.common.helpers import generate_fiscal_month, get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION

from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda
from rest_framework.exceptions import APIException
from usaspending_api.common.helpers import generate_raw_quoted_query


def print_sql(queryset):
    print('=======================================')
    print(generate_raw_quoted_query(queryset))
    print('---')


def award_categories(rows):
    results = {'contracts': 0, 'grants': 0, 'direct_payments': 0, 'loans': 0, 'other': 0}

    categories = {
        'contract': 'contracts',
        'grant': 'grants',
        'direct payment': 'direct_payments',
        'loans': 'loans',
        'other': 'other'
    }

    # DB hit here
    for row in rows:
        if row['category'] is None:
            result_key = 'contracts'
        elif row['category'] not in categories.keys():
            result_key = 'other'
        else:
            result_key = categories[row['category']]
        results[result_key] += row['category_count']

    return results


def spending_over_time_data(request, payload):
    group = payload['group']
    filters = {item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

    queryset = spending_over_time(filters)
    filter_types = filters['award_type_codes'] if 'award_type_codes' in filters else award_type_mapping

    # define what values are needed in the sql query
    queryset = queryset.values('action_date', 'federal_action_obligation', 'original_loan_subsidy_cost')

    nested_order = ''

    group_results = OrderedDict()  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

    if group == 'fy' or group == 'fiscal_year':

        fy_set = sum_transaction_amount(queryset.values('fiscal_year'), filter_types=filter_types)

        for trans in fy_set:
            key = {'fiscal_year': str(trans['fiscal_year'])}
            key = str(key)
            group_results[key] = trans['transaction_amount']

    elif group == 'm' or group == 'month':

        month_set = queryset.annotate(month=ExtractMonth('action_date')) \
            .values('fiscal_year', 'month')
        month_set = sum_transaction_amount(month_set, filter_types=filter_types)

        for trans in month_set:
            # Convert month to fiscal month
            fiscal_month = generate_fiscal_month(date(year=2017, day=1, month=trans['month']))

            key = {'fiscal_year': str(trans['fiscal_year']), 'month': str(fiscal_month)}
            key = str(key)
            group_results[key] = trans['transaction_amount']
        nested_order = 'month'
    else:  # quarterly, take months and add them up

        month_set = queryset.annotate(month=ExtractMonth('action_date')) \
            .values('fiscal_year', 'month')
        month_set = sum_transaction_amount(month_set, filter_types=filter_types)

        for trans in month_set:
            # Convert month to quarter
            quarter = FiscalDate(2017, trans['month'], 1).quarter

            key = {'fiscal_year': str(trans['fiscal_year']), 'quarter': str(quarter)}
            key = str(key)

            # If key exists {fy : quarter}, aggregate
            if group_results.get(key) is None:
                group_results[key] = trans['transaction_amount']
            else:
                if trans['transaction_amount']:
                    group_results[key] = group_results.get(key) + trans['transaction_amount']
                else:
                    group_results[key] = group_results.get(key)
        nested_order = 'quarter'

    # convert result into expected format, sort by key to meet front-end specs
    results = []
    # Expected results structure
    # [{
    # 'time_period': {'fy': '2017', 'quarter': '3'},
    #   'aggregated_amount': '200000000'
    # }]
    sorted_group_results = sorted(
        group_results.items(),
        key=lambda k: (
            ast.literal_eval(k[0])['fiscal_year'],
            int(ast.literal_eval(k[0])[nested_order])) if nested_order else (ast.literal_eval(k[0])['fiscal_year']))

    for key, value in sorted_group_results:
        key_dict = ast.literal_eval(key)
        result = {'time_period': key_dict, 'aggregated_amount': float(value) if value else float(0)}
        results.append(result)
    # build response
    return {'group': group, 'results': results}


def spending_by_award_data(request, payload):
    order = payload.get('order', 'desc')
    limit = payload.get('limit', 50)
    page = payload.get('page', 1)
    fields = payload['fields']
    sort = payload['sort']

    lower_limit = (page - 1) * limit
    upper_limit = page * limit
    filters = {item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

    # build sql query filters
    queryset = matview_search_filter(filters, UniversalAwardView).values()

    values = {'award_id', 'piid', 'fain', 'uri', 'type'}  # always get at least these columns
    for field in fields:
        if award_contracts_mapping.get(field):
            values.add(award_contracts_mapping.get(field))
        if loan_award_mapping.get(field):
            values.add(loan_award_mapping.get(field))
        if non_loan_assistance_award_mapping.get(field):
            values.add(non_loan_assistance_award_mapping.get(field))

    # Modify queryset to be ordered if we specify "sort" in the request
    if sort and "no intersection" not in filters["award_type_codes"]:
        if set(filters["award_type_codes"]) <= set(contract_type_mapping):
            sort_filters = [award_contracts_mapping[sort]]
        elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
            sort_filters = [loan_award_mapping[sort]]
        else:  # assistance data
            sort_filters = [non_loan_assistance_award_mapping[sort]]

        if sort == "Award ID":
            sort_filters = ["piid", "fain", "uri"]
        if order == 'desc':
            sort_filters = ['-' + sort_filter for sort_filter in sort_filters]

        queryset = queryset.order_by(*sort_filters).values(*list(values))

    limited_queryset = queryset[lower_limit:upper_limit + 1]
    has_next = len(limited_queryset) > limit

    results = []
    for award in limited_queryset[:limit]:
        row = {"internal_id": award["award_id"]}

        if award['type'] in loan_type_mapping:  # loans
            for field in fields:
                row[field] = award.get(loan_award_mapping.get(field))
        elif award['type'] in non_loan_assistance_type_mapping:  # assistance data
            for field in fields:
                row[field] = award.get(non_loan_assistance_award_mapping.get(field))
        elif (award['type'] is None and award['piid']) or award['type'] in contract_type_mapping:  # IDV + contract
            for field in fields:
                row[field] = award.get(award_contracts_mapping.get(field))

        if "Award ID" in fields:
            for id_type in ["piid", "fain", "uri"]:
                if award[id_type]:
                    row["Award ID"] = award[id_type]
                    break
        results.append(row)
    # build response
    return {
        'limit': limit,
        'results': results,
        'page_metadata': {
            'page': page,
            'hasNext': has_next
        }
    }


def spending_by_award_count_data(request, payload):
    filters = {item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

    queryset, model = spending_by_award_count(filters)
    if model == 'SummaryAwardView':
        queryset = queryset \
            .values("category") \
            .annotate(category_count=Sum('counts'))
    else:
        # for IDV CONTRACTS category is null. change to contract
        queryset = queryset \
            .values('category') \
            .annotate(category_count=Count(Coalesce('category', Value('contract')))) \
            .values('category', 'category_count')

    # build response
    return {'results': award_categories(list(queryset))}


def award_spending_summary_data(request, payload):
    queryset, model = award_spending_summary(payload)

    if model in ['UniversalAwardView']:
        agg_results = queryset.aggregate(
            award_count=Count('*'),  # surprisingly, this works to create "SELECT COUNT(*) ..."
            award_spending=Sum('total_obligation'))
    else:
        # "summary" materialized views are pre-aggregated and contain a counts col
        agg_results = queryset.aggregate(
            award_count=Sum('counts'),
            award_spending=Sum('total_obligation'))

    results = {
        # The Django Aggregate command will return None if no rows are a match.
        # It is cleaner to return 0 than "None"/null so the values are checked for None
        'prime_awards_count': agg_results['award_count'] or 0,
        'prime_awards_obligation_amount': agg_results['award_spending'] or 0.0,
    }

    return {'results': results}


def spending_by_transaction_data(request, payload):
    order = payload.get('order', 'desc')
    limit = payload.get('limit', 50)
    page = payload.get('page', 1)

    lower_limit = (page - 1) * limit
    upper_limit = page * limit

    filters = {item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

    queryset = matview_search_filter(filters, UniversalTransactionView).values()

    TRANSACTIONS_LOOKUP = {
        'Recipient Name': 'recipient_name',
        'Action Date': 'action_date',
        'Transaction Amount': 'federal_action_obligation',
        'Awarding Agency': 'awarding_toptier_agency_name',
        'Awarding Sub Agency': 'awarding_subtier_agency_name',
        'Funding Agency': 'funding_toptier_agency_name',
        'Funding Sub Agency': 'funding_subtier_agency_name',
        'Issued Date': 'period_of_performance_start_date',
        'Loan Value': 'face_value_loan_guarantee',
        'Subsidy Cost': 'original_loan_subsidy_cost',
        'Mod': 'modification_number',
        'Award ID': 'award_id',
        'awarding_agency_id': 'awarding_agency_id',
        'internal_id': 'award_id',
        'Award Type': 'type',
    }
    TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})

    values = [TRANSACTIONS_LOOKUP[i] for i in payload['fields']]
    sort_filters = [TRANSACTIONS_LOOKUP[payload.get('sort', payload['fields'][0])]]

    if order == 'desc':
        sort_filters = ['-' + sort_filter for sort_filter in sort_filters]

    queryset = queryset.order_by(*sort_filters).values(*list(values))

    limited_queryset = queryset[lower_limit:upper_limit + 1]
    page_metadata = get_simple_pagination_metadata(len(limited_queryset), limit, page)

    results = [
        {TRANSACTIONS_LOOKUP[k]: v for k, v in award.items()} for award in limited_queryset[:limit]
    ]

    # build response
    return {
        'limit': limit,
        'results': results,
        'page_metadata': page_metadata
    }


def transaction_spending_summary_data(request, payload):
    queryset, model = transaction_spending_summary(payload)

    if model in ['UniversalTransactionView']:
        agg_results = queryset.aggregate(
            award_count=Count('*'),  # surprisingly, this works to create "SELECT COUNT(*) ..."
            award_spending=Sum('federal_action_obligation'))
    else:
        # "summary" materialized views are pre-aggregated and contain a counts col
        agg_results = queryset.aggregate(
            award_count=Sum('counts'),
            award_spending=Sum('federal_action_obligation'))

    results = {
        # The Django Aggregate command will return None if no rows are a match.
        # It is cleaner to return 0 than "None"/null so the values are checked for None
        'prime_awards_count': agg_results['award_count'] or 0,
        'prime_awards_obligation_amount': agg_results['award_spending'] or 0.0,
    }

    return {'results': results}


def spending_by_transaction_count_data(request, payload):
    filters = {item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

    queryset, model = transaction_spending_summary(filters)

    if model == 'UniversalTransactionView':
        queryset = queryset \
            .values('award_category') \
            .annotate(
                category_count=Count(Coalesce('award_category', Value('contract'))),
                category=F('award_category')) \
            .values('category', 'category_count')
    else:
        queryset = queryset \
            .values('award_category') \
            .annotate(
                category_count=Sum('counts'),
                category=F('award_category')
            )

    return {'results': award_categories(list(queryset))}
