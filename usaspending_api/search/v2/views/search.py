import logging
import copy

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import ExtractMonth, ExtractYear, Cast, Coalesce

from usaspending_api.awards.models import Subaward
from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import (can_use_view, get_view_queryset, spending_by_award_count,
                                                             spending_by_geography, spending_over_time)
from usaspending_api.awards.v2.lookups.lookups import (award_type_mapping, contract_type_mapping, loan_type_mapping,
                                                       non_loan_assistance_type_mapping, grant_type_mapping,
                                                       contract_subaward_mapping, grant_subaward_mapping)
from usaspending_api.awards.v2.lookups.matview_lookups import (award_contracts_mapping, loan_award_mapping,
                                                               non_loan_assistance_award_mapping)
from usaspending_api.common.exceptions import ElasticsearchConnectionException, InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, generate_fiscal_year, get_simple_pagination_metadata

from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda

from usaspending_api.search.v2.data_layer import spending_by_transaction_count_data, transaction_spending_summary_data, spending_by_transaction_data, award_spending_summary_data, spending_by_award_count_data, spending_by_award_data, spending_over_time_data

from usaspending_api.search.v2.elasticsearch_helper import (search_transactions, spending_by_transaction_count,
                                                            spending_by_transaction_sum_and_count)


logger = logging.getLogger(__name__)


class SpendingOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    endpoint_doc: /advanced_award_search/spending_over_time.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        potential_groups = ['quarter', 'fiscal_year', 'month', 'fy', 'q', 'm']
        models = [
            {'name': 'group', 'key': 'group', 'type': 'enum', 'enum_values': potential_groups},
        ]
        models.extend(AWARD_FILTER)

        validated_payload = TinyShield(models).block(request.data)

        response = spending_over_time_data(request, validated_payload)

        return Response(response)


class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        # TODO: check logic in name_dict[x]["aggregated_amount"] statements

        json_request = request.data
        category = json_request.get("category", None)
        scope = json_request.get("scope", None)
        filters = json_request.get("filters", None)
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        if category is None:
            raise InvalidParameterException("Missing one or more required request parameters: category")
        potential_categories = ["awarding_agency", "funding_agency", "recipient", "cfda_programs", "industry_codes"]
        if category not in potential_categories:
            raise InvalidParameterException("Category does not have a valid value")
        if (scope is None) and (category != "cfda_programs"):
            raise InvalidParameterException("Missing one or more required request parameters: scope")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        # filter queryset
        queryset = matview_search_filter(filters, UniversalTransactionView)

        filter_types = filters['award_type_codes'] if 'award_type_codes' in filters else award_type_mapping

        # filter the transactions by category
        if category == "awarding_agency":
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")

            if scope == "agency":
                queryset = queryset \
                    .filter(awarding_toptier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('awarding_toptier_agency_name'),
                        agency_abbreviation=F('awarding_toptier_agency_abbreviation'))

            elif scope == "subagency":
                queryset = queryset \
                    .filter(
                        awarding_subtier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('awarding_subtier_agency_name'),
                        agency_abbreviation=F('awarding_subtier_agency_abbreviation'))

            elif scope == "office":
                    # NOT IMPLEMENTED IN UI
                    raise NotImplementedError

            queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types)\
                .order_by('-aggregated_amount')
            results = list(queryset[lower_limit:upper_limit + 1])

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "funding_agency":
            potential_scopes = ["agency", "subagency"]
            if scope not in potential_scopes:
                raise InvalidParameterException("scope does not have a valid value")

            if scope == "agency":
                queryset = queryset \
                    .filter(funding_toptier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('funding_toptier_agency_name'),
                        agency_abbreviation=F('funding_toptier_agency_abbreviation'))

            elif scope == "subagency":
                queryset = queryset \
                    .filter(
                        funding_subtier_agency_name__isnull=False) \
                    .values(
                        agency_name=F('funding_subtier_agency_name'),
                        agency_abbreviation=F('funding_subtier_agency_abbreviation'))

            elif scope == "office":
                # NOT IMPLEMENTED IN UI
                raise NotImplementedError

            queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                .order_by('-aggregated_amount')
            results = list(queryset[lower_limit:upper_limit + 1])

            page_metadata = get_simple_pagination_metadata(len(results), limit, page)
            results = results[:limit]

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "recipient":
            if scope == "duns":
                queryset = queryset \
                    .values(legal_entity_id=F("recipient_id"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .order_by('-aggregated_amount') \
                    .values("aggregated_amount", "legal_entity_id", "recipient_name") \
                    .order_by("-aggregated_amount")

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            elif scope == "parent_duns":
                queryset = queryset \
                    .filter(parent_recipient_unique_id__isnull=False)
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types,
                                                  calculate_totals=False) \
                    .values(
                        'aggregated_amount',
                        'recipient_name',
                        'parent_recipient_unique_id') \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")

            response = {"category": category, "scope": scope, "limit": limit, "results": results,
                        "page_metadata": page_metadata}
            return Response(response)

        elif category == "cfda_programs":
            if can_use_view(filters, 'SummaryCfdaNumbersView'):
                queryset = get_view_queryset(filters, 'SummaryCfdaNumbersView')
                queryset = queryset \
                    .filter(
                        federal_action_obligation__isnull=False,
                        cfda_number__isnull=False) \
                    .values(cfda_program_number=F("cfda_number"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .values(
                        "aggregated_amount",
                        "cfda_program_number",
                        program_title=F("cfda_title")) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]
                for trans in results:
                    trans['popular_name'] = None
                    # small DB hit every loop here
                    cfda = Cfda.objects \
                        .filter(
                            program_title=trans['program_title'],
                            program_number=trans['cfda_program_number']) \
                        .values('popular_name').first()

                    if cfda:
                        trans['popular_name'] = cfda['popular_name']

            else:
                queryset = queryset \
                    .filter(
                        cfda_number__isnull=False) \
                    .values(cfda_program_number=F("cfda_number"))
                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .values(
                        "aggregated_amount",
                        "cfda_program_number",
                        popular_name=F("cfda_popular_name"),
                        program_title=F("cfda_title")) \
                    .order_by('-aggregated_amount')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])
                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

            response = {"category": category, "limit": limit, "results": results, "page_metadata": page_metadata}
            return Response(response)

        elif category == "industry_codes":  # industry_codes
            if scope == "psc":
                if can_use_view(filters, 'SummaryPscCodesView'):
                    queryset = get_view_queryset(filters, 'SummaryPscCodesView')
                    queryset = queryset \
                        .filter(product_or_service_code__isnull=False) \
                        .values(psc_code=F("product_or_service_code"))
                else:
                    queryset = queryset \
                        .filter(psc_code__isnull=False) \
                        .values("psc_code")

                queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                    .order_by('-aggregated_amount')
                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            elif scope == "naics":
                if can_use_view(filters, 'SummaryNaicsCodesView'):
                    queryset = get_view_queryset(filters, 'SummaryNaicsCodesView')
                    queryset = queryset \
                        .filter(naics_code__isnull=False) \
                        .values('naics_code')
                    queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                        .order_by('-aggregated_amount') \
                        .values(
                            'naics_code',
                            'aggregated_amount',
                            'naics_description')
                else:
                    queryset = queryset \
                        .filter(naics_code__isnull=False) \
                        .values("naics_code")
                    queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
                        .order_by('-aggregated_amount') \
                        .values(
                            'naics_code',
                            'aggregated_amount',
                            'naics_description')

                # Begin DB hits here
                results = list(queryset[lower_limit:upper_limit + 1])

                page_metadata = get_simple_pagination_metadata(len(results), limit, page)
                results = results[:limit]

                response = {"category": category, "scope": scope, "limit": limit, "results": results,
                            "page_metadata": page_metadata}
                return Response(response)

            else:  # recipient_type
                raise InvalidParameterException("recipient type is not yet implemented")


class SpendingByGeographyVisualizationViewSet(APIView):
    """
        This route takes award filters, and returns spending by state code, county code, or congressional district code.
        endpoint_doc: /advanced award search/spending_by_geography.md
    """
    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
        json_request = request.data

        self.subawards = json_request.get("subawards", False)
        self.scope = json_request.get("scope")
        self.filters = json_request.get("filters", {})
        self.geo_layer = json_request.get("geo_layer")
        self.geo_layer_filters = json_request.get("geo_layer_filters")

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {
            'state': 'state_code',
            'county': 'county_code',
            'district': 'congressional_code'
        }

        model_dict = {
            'place_of_performance': 'pop',
            'recipient_location': 'recipient_location',
            'subawards_place_of_performance': 'place_of_performance',
            'subawards_recipient_location': 'recipient__location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get('{}{}'.format('subawards_' if self.subawards else '', self.scope))
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = '{}_{}{}'.format(scope_field_name, '_' if self.subawards else '', loc_field_name)

        if scope_field_name is None:
            raise InvalidParameterException("Invalid request parameters: scope")
        if loc_field_name is None:
            raise InvalidParameterException("Invalid request parameters: geo_layer")
        if type(self.subawards) is not bool:
            raise InvalidParameterException('subawards does not have a valid value')

        if self.subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.queryset = subaward_filter(self.filters)
            self.model_name = Subaward
        else:
            self.queryset, self.model_name = spending_by_geography(self.filters)

        if self.geo_layer == 'state':
            # State will have one field (state_code) containing letter A-Z
            kwargs = {
                '{}_{}country_code'.format(scope_field_name, '_location_' if self.subawards else ''): 'USA',
                '{}'.format('amount__isnull' if self.subawards else 'federal_action_obligation__isnull'): False
            }

            # Only state scope will add its own state code
            # State codes are consistent in database i.e. AL, AK
            fields_list.append(loc_lookup)

            state_response = {
                'scope': self.scope,
                'geo_layer': self.geo_layer,
                'results': self.state_results(kwargs, fields_list, loc_lookup)
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = '{}_{}{}'.format(scope_field_name, '_' if self.subawards else '', loc_dict['state'])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {
                '{}__isnull'.format('amount' if self.subawards else 'federal_action_obligation'): False
            }

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name_lookup = '{}_{}county_name'.format(scope_field_name, '_' if self.subawards else '')
                fields_list.append(county_name_lookup)
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                county_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.county_results(state_lookup, county_name_lookup)
                }

                return Response(county_response)
            else:
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                district_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.district_results(state_lookup)
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields, loc_lookup):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{'{}__{}'.format(loc_lookup, 'in'): self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args['{}__isnull'.format(loc_lookup)] = False

        self.geo_queryset = self.queryset.filter(**filter_args).values(*lookup_fields)
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types) if not self.subawards \
            else self.geo_queryset.annotate(transaction_amount=Sum('amount'))

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [{
            'shape_code': x[loc_lookup],
            'aggregated_amount': x['transaction_amount'],
            'display_name': code_to_state.get(x[loc_lookup], {'name': 'None'}).get('name').title()
        } for x in self.geo_queryset]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, state_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            self.queryset &= geocode_filter_locations(scope_field_name, [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ], self.model_name, not self.subawards)
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs['{}__{}'.format(loc_lookup, 'isnull')] = False
            kwargs['{}__{}'.format(state_lookup, 'isnull')] = False
            kwargs['{}_{}country_code'.format(scope_field_name, '_location_' if self.subawards else '')] = 'USA'
            kwargs['{}__{}'.format(loc_lookup, 'iregex')] = r'^[0-9]*(\.\d+)?$'

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list) \
            .annotate(code_as_float=Cast(loc_lookup, FloatField()))
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types) if not self.subawards \
            else self.geo_queryset.annotate(transaction_amount=Sum('amount'))

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [{
            'shape_code': code_to_state.get(x[state_lookup])['fips'] + pad_codes(self.geo_layer, x['code_as_float']),
            'aggregated_amount': x['transaction_amount'],
            'display_name': x[county_name].title() if x[county_name] is not None else x[county_name]
        } for x in self.geo_queryset]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [{
            'shape_code': code_to_state.get(x[state_lookup])['fips'] + pad_codes(self.geo_layer, x['code_as_float']),
            'aggregated_amount': x['transaction_amount'],
            'display_name': x[state_lookup] + '-' + pad_codes(self.geo_layer, x['code_as_float'])
        } for x in self.geo_queryset]

        return results


class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    endpoint_doc: /advanced_award_search/spending_by_award.md
    """

    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
        ]
        models.extend(AWARD_FILTER)
        models.extend(PAGINATION)
        for m in models:
            if m['name'] in ('award_type_codes', 'sort'):
                m['optional'] = False
        validated_payload = TinyShield(models).block(request.data)

        if validated_payload['sort'] not in validated_payload['fields']:
            raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload['sort']))

        response = spending_by_award_data(request, validated_payload)
        return Response(response)


class SpendingByAwardCountVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Grants, etc.)
        endpoint_doc: /advanced_award_search/spending_by_award_count.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        models = copy.copy(AWARD_FILTER)
        validated_payload = TinyShield(models).block(request.data)
        response = spending_by_award_count_data(request, validated_payload)

        return Response(response)


class AwardSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards and summation of total obligations.
        endpoint_doc: /advanced_award_search/award_spending_summary.md
    """
    @cache_response()
    def post(self, request):
        """
            Returns a summary of awards which match the award search filter
                Desired values:
                    total number of awards `award_count`
                    The total_obligation sum of all those awards `award_spending`
            *Note* Only deals with prime awards, future plans to include sub-awards.
        """

        models = copy.copy(AWARD_FILTER)
        validated_payload = TinyShield(models).block(request.data)
        response = award_spending_summary_data(request, validated_payload)

        return Response(response)


class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction.md
    """

    @cache_response()
    def post(self, request):

        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
        ]
        models.extend(AWARD_FILTER)
        models.extend(PAGINATION)
        for m in models:
            if m['name'] in ('award_type_codes', 'sort'):
                m['optional'] = False
        validated_payload = TinyShield(models).block(request.data)

        if validated_payload['sort'] not in validated_payload['fields']:
            raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload['sort']))

        response = spending_by_transaction_data(request, validated_payload)
        return Response(response)


class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction_count.md

    """
    @cache_response()
    def post(self, request):

        models = copy.copy(AWARD_FILTER)
        validated_payload = TinyShield(models).block(request.data)
        response = spending_by_transaction_count_data(request, validated_payload)

        return Response(response)


class TransactionSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of transactions and summation of federal action obligations.
        endpoint_doc: /advanced_award_search/transaction_spending_summary.md
    """
    @cache_response()
    def post(self, request):
        """
            Returns a summary of transactions which match the award search filter
                Desired values:
                    total number of transactions `award_count`
                    The federal_action_obligation sum of all those transactions `award_spending`
            *Note* Only deals with prime awards, future plans to include sub-awards.
        """

        models = copy.copy(AWARD_FILTER)
        validated_payload = TinyShield(models).block(request.data)
        response = transaction_spending_summary_data(request, validated_payload)

        return Response(response)
