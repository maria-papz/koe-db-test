import json
from django.http import JsonResponse
from django.core.exceptions import FieldDoesNotExist, FieldError
from django.db.models import Q, ForeignKey
from django.db import transaction
from koe_db.authentication import CustomJWTAuthentication

from django.apps import apps
# TODO: Bug when nonetype in data
# TODO: Frequency issue

from .models import (
    AccessLevel, IndicatorPermission, ActionLog, Country, CustomTable,
    Data, Indicator, Category, Region, CustomIndicator, Unit, UserAccount, UserFavouriteIndicators, UserFavouriteTables, UserFollowsUser, Frequency
)
from .permissions import (
    check_indicator_permission,
    check_custom_indicator_permission, check_table_view_permission, get_accessible_indicators, get_accessible_tables,
    initialize_indicator_access
)

def sql_indicator_query(request):
    try:
        user = request.get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)
        if request.method == 'POST':
            data = json.loads(request.body)
            query = data.get('query')
            # Validate query to ensure it's a SELECT statement only
            query = query.strip()
            # Block potentially harmful operations
            disallowed_keywords = ['insert', 'update', 'delete', 'drop', 'alter', 'truncate', 'create', 'replace']
            if any(keyword in query.lower() for keyword in disallowed_keywords):
                return JsonResponse({'error': 'Query contains disallowed operations'}, status=403)
            indicators = Indicator.objects.raw(query)
            indicator_list = []
            for indicator in indicators:
                if not check_indicator_permission(user,indicator,'view'):
                    continue
                indicator_list.append({
                    'id': indicator.id,
                    'name': indicator.name,
                    'code': indicator.code,
                    'base_year': indicator.base_year,
                    'description': indicator.description,
                    'source': indicator.source,
                    'category': getattr(indicator.category, 'name', None),
                    'country': getattr(indicator.country, 'name', None),
                    'unit': getattr(indicator.unit, 'name', None),
                    'region': getattr(indicator.region, 'name', None),
                    'is_seasonally_adjusted': indicator.seasonally_adjusted,
                    'frequency': indicator.frequency,
                    'is_custom': indicator.is_custom,
                    'currentPrices': indicator.currentPrices
                })
            return JsonResponse(indicator_list, safe=False)
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)


def duplicate_indicator(indicator_id,request):
    indicator_to_duplicate = Indicator.objects.get(id=indicator_id)
    if not indicator_to_duplicate:
        return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)
    if request.method == 'POST':
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)
        if not check_indicator_permission(user, indicator_to_duplicate, 'edit'):
            return JsonResponse({'error': 'User does not have permission to duplicate this indicator'}, status=403)

        new_name = request.body.get('name')
        new_code = request.body.get('code')

        new_indicator = Indicator.objects.create(
            name=new_name,
            code=new_code,
            description=indicator_to_duplicate.description,
            source=indicator_to_duplicate.source,
            category=indicator_to_duplicate.category,
            country=indicator_to_duplicate.country,
            region=indicator_to_duplicate.region,
            base_year=indicator_to_duplicate.base_year,
            seasonally_adjusted=indicator_to_duplicate.seasonally_adjusted,
            frequency=indicator_to_duplicate.frequency,
            is_custom=indicator_to_duplicate.is_custom,
            currentPrices=indicator_to_duplicate.currentPrices,
            unit=indicator_to_duplicate.unit
        )

        # indicator create action log
        ActionLog.objects.create(
            user=user,
            indicator=new_indicator,
            action_type='INDICATOR_CREATE',
            details={'name': new_name, 'code': new_code}
        )
        # Copy history
        history = ActionLog.objects.filter(indicator=indicator_to_duplicate)
        for h in history:
            ActionLog.objects.create(
                user=h.user,
                indicator=new_indicator,
                action_type=h.action_type,
                details=h.details
            )

        # Copy data
        data = Data.objects.filter(indicator=indicator_to_duplicate)
        for d in data:
            Data.objects.create(
                indicator=new_indicator,
                period=d.period,
                value=d.value
            )
        # Copy permissions
        permissions = IndicatorPermission.objects.filter(indicator=indicator_to_duplicate)
        for p in permissions:
            IndicatorPermission.objects.create(
                user=p.user,
                indicator=new_indicator,
                can_view=p.can_view,
                can_edit=p.can_edit,
                can_delete=p.can_delete
            )

        return JsonResponse({'success': 'Indicator duplicated successfully', 'indicator_id': new_indicator.id})

def get_user(request):
    auth = CustomJWTAuthentication()
    user = auth.authenticate(request)
    if not user:
        return None
    else:

        user_account = UserAccount.objects.get(id = user[0].id)
        return user_account




def codes(request):
    if request.method == 'GET':
        try:
            indicators = Indicator.objects.values_list('code', flat=True)
            print(indicators)
            return JsonResponse(list(indicators), safe=False)
        except Exception as e:
            print(e)
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=400)

def add_view_category(request):
    try:
        if request.method == 'GET':
            # get category fields to respond in json
            categories = Category.objects.all()
            category_list = []
            for category in categories:
                print(category.id)
                category_list.append({
                    'id': category.id,
                    'name': category.name,
                    'description': category.description
                })
            return JsonResponse(category_list, safe=False)
        elif request.method == 'POST':
            data = json.loads(request.body)
            name = data.get('name')
            print(name)
            description = data.get('description')
            print(description)
            category = Category.objects.create(name=name, description=description)
            return JsonResponse({'success': 'Category created successfully', 'id': category.id})
        else:
            return JsonResponse({'error': 'Invalid request method'}, status=400)
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def add_view_unit(request):
    try:
        if request.method == 'GET':
            # get category fields to respond in json
            units = Unit.objects.all()
            unit_list = []
            for unit in units:
                print(unit.id)
                unit_list.append({
                    'id': unit.id,
                    'name': unit.name,
                    'description': unit.description,
                    'symbol': unit.symbol
                })
            return JsonResponse(unit_list, safe=False)
        elif request.method == 'POST':
            data = json.loads(request.body)
            name = data.get('name')
            print(name)
            description = data.get('description')
            print(description)
            symbol = data.get('symbol')
            print(symbol)
            unit = Unit.objects.create(name=name, description=description, symbol=symbol)
            return JsonResponse({'success': 'Unit created successfully', 'id': unit.id})
        else:
            return JsonResponse({'error': 'Invalid request method'}, status=400)
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def country_codes(request):
    if request.method == 'GET':
        try:
            countries = Country.objects.values_list('code', flat=True)
            return JsonResponse(list(countries), safe=False)
        except Exception as e:
            print(e)
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=400)


def add_view_country(request):
    try:
        if request.method == 'GET':
            countries = Country.objects.all()
            country_list = []
            for country in countries:
                country_list.append({
                    'id': country.id,
                    'name': country.name,
                    'code': country.code,
                    'regions': [region.name for region in country.regions.all()] if country.regions else []
                })
            return JsonResponse(country_list, safe=False)
        elif request.method == 'POST':
            data = json.loads(request.body)
            name = data.get('name')
            code = data.get('code')
            regions = data.get('regions')
            country = Country.objects.create(name=name, code=code)
            for region_id in regions:
                region = Region.objects.get(id=region_id)
                country.regions.add(region)
            return JsonResponse({'success': 'Country created successfully', 'id': country.id})
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def add_view_region(request):
    try:
        if request.method == 'GET':
            regions = Region.objects.all()
            region_list = []
            for region in regions:
                region_list.append({
                    'id': region.id,
                    'name': region.name,
                    'description': region.description
                })
            return JsonResponse(region_list, safe=False)
        elif request.method == 'POST':
            data = json.loads(request.body)
            name = data.get('name')
            description = data.get('description')
            region = Region.objects.create(name=name, description=description)
            return JsonResponse({'success': 'Region created successfully', 'id': region.id})
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def get_available_fields(request):
    field_choices = []
    app_models = apps.get_app_config('koe_db').get_models()
    for model in app_models:
        for field in model._meta.fields:
            if field.get_internal_type() in ['CharField', 'IntegerField', 'FloatField', 'DecimalField']:
                field_label = f"{model._meta.verbose_name} - {field.verbose_name}"
                field_name = f"{model._meta.model_name}__{field.name}"
                field_choices.append({'value': field_name, 'label': field_label})
    return JsonResponse(field_choices, safe=False)


# write an efficient filter/search function for indicators
def boolean_filter(request):
    try:
        if request.method == 'POST':
            search_data = json.loads(request.body)
            base= search_data.get('base')
            # add boolean to base
            base['boolean'] = ''
            fields = search_data.get('additionalFields', [])
            # append base in front of fields
            fields.insert(0, base)
            print(fields)
            grouped_results = {}
            allowed_indicators = get_accessible_indicators(get_user(request))
            # fields include a field denoted as model_name__field_name called field a value, which corresponds to the search term matching the field and boolean operator which is how different search criteria are combined
            # boolean operator can be AND, OR or NOT (NOT is basically AND NOT)
            # for example if fields is [{'field': 'indicator__name', 'value': 'Nikkei', 'boolean_operator': ''}, {'field': 'indicator__source', 'value': 'European Central Bank', 'boolean_operator': 'OR'}]
            # this should return all indicators that have name containing 'Nikkei' or source containing 'European Central Bank'
            # first field never has a boolean operator
            # there can be several conditions
            for index, f in enumerate(fields):
                field = f['field']
                value = f['value']
                boolean_operator = f['boolean']
                results = {}

                # results should only be indicators, but we still need to check all related tables
                if field == 'description' or field == 'source' or field == 'name' or field == 'code' or field == 'base_year' or field == 'source':
                    results = Indicator.objects.filter(Q(**{f'{field}__icontains': value}))
                # go through models until we reach the indicator model record that contains the related table record with that value
                # for example if we are searching for a country code, we need to go through the country model find the countries with that code and then find that country as a foreign key in the indicator model
                # this is a recursive function that goes through all related tables until it reaches the indicator model
                else:
                    results = search(field, value)
                # apply set operations to results
                if boolean_operator == '':
                    print(f'no boolean operator {index}')
                    grouped_results = {'records': results}
                elif boolean_operator == 'AND':
                    print(f'AND {index}')
                    grouped_results = {'records': (set(results) & set(grouped_results['records']) & set(allowed_indicators))}
                elif boolean_operator == 'OR':
                    print(f'OR {index}')
                    grouped_results = {'records': (set(results) | set(grouped_results['records']) & set(allowed_indicators))}
                elif boolean_operator == 'NOT':
                    print(f'NOT {index}')
                    grouped_results = {'records': (set(grouped_results['records']) - set(results)) & set(allowed_indicators)}
            # print indicator names from grouped_results
            # for indicator in grouped_results['records']:
            #     print(indicator.name)

            grouped_by_frequency = {}
            for indicator in grouped_results['records']:
                # print(indicator.data_frequency_years)
                frequency = dict(Frequency.choices).get(indicator.frequency)
                if frequency not in grouped_by_frequency:
                    grouped_by_frequency[frequency] = []
                grouped_by_frequency[frequency].append(indicator)
            # print indicator names from grouped_by_frequency
            final_json = {}
            # these grouped results should be returned as a json response, the response should have an appropriate structure so frontend can read the indicator name, code and id grouped by date frequency. The frontend should be able to display this information as a selectable list
            # it should appear as n lists where n is the nunber of different data frequencies, each list should have indicator name id and code
            for frequency, indicators in grouped_by_frequency.items():
                print(frequency)
                list_of_indicators = []
                for indicator in indicators:
                    list_of_indicators.append({'id': indicator.id, 'name': indicator.name, 'code': indicator.code})
                final_json[frequency] = list_of_indicators
            # print indented json response
            # print(json.dumps(final_json, indent=4))
            return JsonResponse(final_json)



    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)
def delete_table_indicator(request, table_id, indicator_id):
    try:
        if request.method == 'DELETE':
            table = CustomTable.objects.get(id=table_id)
            if not table:
                return JsonResponse({'error': f'Table with id {table_id} not found'}, status=404)
            indicator = Indicator.objects.get(id=indicator_id)
            if not indicator:
                return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)
            table.indicators.remove(indicator)
            return JsonResponse({'success': f'Indicator {indicator_id} removed from table {table_id}'})
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

# define recursive function for filtering through related tables

from django.db.models import Q
from django.http import HttpResponse

def search(field, search_value):
    results = set()  # Use a set to avoid duplicate results

    if field.lower() == 'seasonally_adjusted':
        indicators = Indicator.objects.filter(Q(**{f"seasonally_adjusted": search_value.lower() == 'true'}))
        results.update(indicators)

    elif field.lower() == 'is_custom':
        indicators = Indicator.objects.filter(Q(**{f"is_custom": search_value.lower() == 'true'}))
        results.update(indicators)

    elif field.lower() == 'currentprices':
        indicators = Indicator.objects.filter(Q(**{f"currentPrices": search_value.lower() == 'true'}))
        print('')
        print('und',indicators)
        results.update(indicators)

    elif field.lower() == 'frequency' or field.lower() == 'data_frequency_years':
        # Convert the search value to its corresponding frequency code
        frequency_code = None
        # Map the search term to the appropriate frequency code
        search_value_lower = search_value.lower()
        for code, label in Frequency.choices:
            if label.lower() in search_value_lower or search_value_lower in label.lower():
                frequency_code = code
                break



        if frequency_code:
            indicators = Indicator.objects.filter(Q(**{f"frequency": frequency_code})) | Indicator.objects.filter(Q(**{f"other_frequency__icontains": search_value}))
        else:
            indicators = Indicator.objects.filter(Q(**{f"other_frequency__icontains": search_value}))
        results.update(indicators)

    elif field.lower() == 'unit':
        indicators = Indicator.objects.filter(Q(**{f"unit__name__icontains": search_value}))
        results.update(indicators)

    elif field.lower() == 'category':
        # Find Indicators linked to the specified Category field
        indicators = Indicator.objects.filter(Q(**{f"category__name__icontains": search_value}))
        results.update(indicators)

    elif field.lower() == 'country':
        # Find Indicators referencing the specified Country field
        indicators = Indicator.objects.filter(Q(**{f"country__name__icontains": search_value}))
        results.update(indicators)

    elif field.lower() == 'region':
        # Find Indicators referencing the specified Region field directly
        indicators = Indicator.objects.filter(Q(**{f"region__name__icontains": search_value}))
        results.update(indicators)

        # Also find Countries related to this Region and then find their Indicators
        countries = Country.objects.filter(Q(**{f"regions__name__icontains": search_value}))
        indicators_from_countries = Indicator.objects.filter(country__in=countries)
        results.update(indicators_from_countries)

    else:
        print("Invalid model name provided.")

    # Return the results as a list
    return results


def add_view_indicators(request):
    try:
        if request.method == 'GET':
            if not get_user(request):
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            # Instead of all indicators, get only accessible ones
            user = get_user(request)
            indicators = get_accessible_indicators(user)
            print(indicators)

            indicator_list = []
            for indicator in indicators:
                # ...existing code to create indicator_list...
                try:
                    access_level = indicator.access_level.level
                except:
                    access_level = 'org_full_public'

                region_list = []
                if indicator.region:
                    region_list = [indicator.region.name]
                elif indicator.country:
                    region_list = [region.name for region in indicator.country.regions.all()]

                if indicator.frequency == 'CUSTOM':
                    frequency = indicator.other_frequency
                else:
                    frequency = dict(Frequency.choices).get(indicator.frequency)

                edit_permission=check_indicator_permission(user, indicator, 'edit')
                delete_permission=check_indicator_permission(user, indicator, 'delete')
                indicator_list.append({
                    'id': indicator.id,
                    'name': indicator.name,
                    'code': indicator.code,
                    'base_year': indicator.base_year,
                    'description': indicator.description,
                    'source': indicator.source,
                    'category': getattr(indicator.category, 'name', None),
                    'country': getattr(indicator.country, 'name', None),
                    'unit': getattr(indicator.unit, 'name', None),
                    'region': region_list,
                    'is_seasonally_adjusted': indicator.seasonally_adjusted,
                    'frequency': frequency,
                    'is_custom': indicator.is_custom,
                    'currentPrices': indicator.currentPrices,
                    'access_level': access_level,
                    'edit': edit_permission,
                    'delete': delete_permission,
                    'is_favourite': UserFavouriteIndicators.objects.filter(user=user, indicators=indicator).exists()
                })

            # ...existing code for metadata_set...
            metadata_set = {}
            for indicator in indicator_list:
                for key, value in indicator.items():
                    if key not in metadata_set:
                        metadata_set[key] = set()
                    if isinstance(value, list):
                        metadata_set[key].update(value)
                    else:
                        metadata_set[key].add(value)

            for key in metadata_set:
                metadata_set[key] = list(metadata_set[key])
            return JsonResponse({'indicators':indicator_list,'metadataset':metadata_set}, safe=False)

        elif request.method == 'POST':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)

            # Extract access level information
            access_level = data.get('access_level', AccessLevel.ORG_FULL_PUBLIC)

            # Create the indicator as before
            name = data.get('name')
            code= data.get('code')
            description = data.get('description')
            category_id = data.get('category')
            country_id = data.get('country')
            region_id = data.get('region')
            base_year = data.get('base_year')
            unit_id = data.get('unit')
            other_frequency = data.get('other_frequency', None)
            source = data.get('source', 'manual entry')
            seasonally_adjusted = data.get('seasonally_adjusted') == 'true'
            frequency = data.get('frequency')
            is_custom = data.get('is_custom') == 'true'
            current_prices = data.get('current_prices') == 'true'
            print(data)
            category = Category.objects.get(id=category_id) if category_id else None
            country = Country.objects.get(id=country_id) if country_id else None
            region = Region.objects.get(id=region_id) if region_id else None
            unit_obj = Unit.objects.get(id=unit_id) if unit_id else None

            # Create the indicator with a transaction to ensure both indicator and access level are created
            with transaction.atomic():
                indicator = Indicator.objects.create(
                    name=name,
                    code=code,
                    description=description,
                    category=category,
                    country=country,
                    region=region,
                    base_year=base_year,
                    source=source,
                    seasonally_adjusted=seasonally_adjusted,
                    frequency=frequency,
                    is_custom=is_custom,
                    currentPrices=current_prices,
                    unit=unit_obj,
                    other_frequency=other_frequency
                )

                # Set access level
                initialize_indicator_access(indicator, access_level)

                # Create user permissions for restricted access
                if access_level == AccessLevel.RESTRICTED:
                    # Assign false permissions to all non-superusers
                    for u in UserAccount.objects.filter(is_superuser=False):
                        IndicatorPermission.objects.create(
                            user=u,
                            indicator=indicator,
                            can_view=False,
                            can_edit=False,
                            can_delete=False
                        )
                    # Give full permissions to the current user
                    if user:
                        IndicatorPermission.objects.create(
                            user=user,
                            indicator=indicator,
                            can_view=True,
                            can_edit=True,
                            can_delete=True
                        )

                # Create log entry
                ActionLog.objects.create(
                    user=user,
                    indicator=indicator,
                    action_type='INDICATOR_CREATE',
                    details={'name': name, 'code': code}
                )

            return JsonResponse({'success': 'Indicator created successfully', 'indicator_id': indicator.id})
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)


def tables(request, id):
    try:
        if request.method == 'GET':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            # get CustomTable model of given id
            print(id)
            table = CustomTable.objects.get(id=id)
            if not check_table_view_permission(user, table):
                return JsonResponse({'error': 'User does not have permission to view this table'}, status=403)
            if not table:
                return JsonResponse({'error': f'Table with id {id} not found'})
            table_name = table.name
            table_description = table.description
            # return indicator names and it fields for given table
            indicators = table.indicators.all()
            # return data for each indicator, indicator metadata should also be part of the structure
            data = {}
            for indicator in indicators:
                try:
                    data[indicator.name] = Data.objects.filter(indicator=indicator)
                except Exception as e:
                    print(e)
                    continue
            # data dictionary should also group by period
            data_by_period = {}
            for indicator, values in data.items():
                for value in values:
                    period = value.period
                    if period not in data_by_period:
                        data_by_period[period] = {}
                    data_by_period[period][indicator] = value.value
            # data period is ascending
            data_by_period = dict(sorted(data_by_period.items()))
            # data by period should be turned into an array where each row is date, indicator1, indicator2 and so on
            data_by_indicators = []
            all_indicators = set(indicators.values_list('name', flat=True))
            for period, values in data_by_period.items():
                row = {'period': period}
                for indicator in all_indicators:
                    row[indicator] = values.get(indicator, None)
                data_by_indicators.append(row)
            print(data_by_indicators)
            # return metadata for each indicator
            indicator_metadata = {}
            for indicator in indicators:
                indicator_metadata[indicator.name] = {
                    'id': indicator.id,
                    'name': indicator.name,
                    'code': indicator.code,
                    'base_year': indicator.base_year,
                    'description': indicator.description,
                    'source': indicator.source,
                    'category': getattr(indicator.category, 'name', None),
                    'country': getattr(indicator.country, 'name', None),
                    'region': getattr(indicator.region, 'name', None),
                    'unit': getattr(indicator.unit, 'name', None),
                    'is_seasonally_adjusted': indicator.seasonally_adjusted,
                    'frequency': indicator.frequency,
                    'other_frequency': indicator.other_frequency,
                    'is_custom': indicator.is_custom,
                    'currentPrices': indicator.currentPrices
                }
            # return structured json response to this, this should include information about the table, indicators and data
            return JsonResponse({'table_name': table_name, 'table_description': table_description, 'indicators': indicator_metadata, 'data': data_by_indicators})
        elif request.method == 'DELETE':
            table = CustomTable.objects.get(id=id)
            if not table:
                return JsonResponse({'error': f'Table with id {id} not found'}, status=404)
            table.delete()
            return JsonResponse({'success': 'Table deleted successfully'}, status=204)
        elif request.method == 'PATCH':
            data = json.loads(request.body)
            table = CustomTable.objects.get(id=id)
            if not table:
                return JsonResponse({'error': f'Table with id {id} not found'}, status=404)
            table_name = data.get('table_name')
            table_description = data.get('table_description')
            table.name = table_name
            table.description = table_description
            table.save()
            return JsonResponse({'success': 'Table updated successfully', 'table_name': table.name, 'table_description': table.description})
        else:
            return JsonResponse({'error': 'Invalid request  '} , status=400)

    except Exception as e:
        print(e)
        return JsonResponse({'error rendering table': str(e)})


def add_view_table(request):
    try:
        if request.method == 'POST':
            data = json.loads(request.body)
            table_name = data.get('table_name')
            table_description = data.get('table_description')
            table = CustomTable.objects.create(name=table_name, description=table_description)
            return JsonResponse({'success': 'Table created successfully',
                                'table_name': table.name,
                                'table_description': table.description,
                                'table_id': table.id})
        elif request.method == 'GET':
            all_tables = CustomTable.objects.all()
            tables = get_accessible_tables(get_user(request))
            table_list = []
            table_metadata = []
            for table in tables:
                table_list.append({
                    'id': table.id,
                    'name': table.name,
                    'description': table.description,
                    'indicators': [indicator.code for indicator in table.indicators.all()],
                    'is_favourite': UserFavouriteTables.objects.filter(user=get_user(request), tables=table).exists()
                })
                table_metadata.append({
                    'id': table.id,
                    'indicator_regions': [[getattr(indicator.region, 'name', None)] + [region.name for region in indicator.country.regions.all()] if indicator.country else [getattr(indicator.region, 'name', None)] for indicator in table.indicators.all()],
                    'indicator_country': [getattr(indicator.country, 'name', None) for indicator in table.indicators.all() if indicator.country] if table.indicators.all() else [],
                    'indicator_unit': [getattr(indicator.unit, 'name', None) for indicator in table.indicators.all() if indicator.unit] if table.indicators.all() else [],
                    'indicator_code': [indicator.code for indicator in table.indicators.all()],
                    'indicator_names': [indicator.name for indicator in table.indicators.all()],
                    'indicator_is_custom': [indicator.is_custom for indicator in table.indicators.all()],
                    'indicator_frequency': [indicator.frequency for indicator in table.indicators.all()],
                    'indicator_currentPrices': [indicator.currentPrices for indicator in table.indicators.all()],
                    'indicator_source': [indicator.source for indicator in table.indicators.all()],
                    'indicator_category': [getattr(indicator.category, 'name', None) for indicator in table.indicators.all()],
                    'indicator_base_year': [indicator.base_year for indicator in table.indicators.all()],
                    'indicator_seasonally_adjusted': [indicator.seasonally_adjusted for indicator in table.indicators.all()]
                })
            metadata_set = {}
            # loop through each metadata item. go through the each list in each field and group into a big list. once the big list is complete, remove duplicates and return the list
            for metadata in table_metadata:
                for key, value in metadata.items():
                    if key not in metadata_set:
                        metadata_set[key] = set()
                    if isinstance(value, list):
                        if key == 'indicator_regions':
                            for sublist in value:
                                metadata_set[key].update(sublist)
                        else:
                            metadata_set[key].update(value)
                    else:
                        metadata_set[key].add(value)
            # Convert sets to lists
            for key in metadata_set:
                metadata_set[key] = list(metadata_set[key])
            return JsonResponse({'table':table_list,'metadata':table_metadata,'metadata_set':metadata_set}, safe=False)
        else:
            return JsonResponse({'error': 'Invalid request'}, status=400)
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def indicator_history(request, indicator_id):
    try:
        if request.method == 'GET':
            indicator = Indicator.objects.get(id=indicator_id)
            if not check_indicator_permission(get_user(request), indicator, 'view'):
                print('denied!!!!!!!')
                return JsonResponse({'error': 'User does not have permission to view this indicator'}, status=403)
            if not indicator:
                return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)

            action_types = ['DATA_UPDATE', 'INDICATOR_EDIT']
            if indicator.is_custom:
                action_types.append('FORMULA_UPDATE')

            action_logs = ActionLog.objects.filter(indicator=indicator, action_type__in=action_types).values('timestamp', 'details', 'action_type', 'user')

            grouped_by_timestamp = {}
            for entry in action_logs:
                timestamp = entry['timestamp']
                details = entry['details']
                user_id = entry['user']
                user = UserAccount.objects.get(id=user_id) if user_id else None
                user_email = user.email if user else None
                if timestamp not in grouped_by_timestamp:
                    grouped_by_timestamp[timestamp] = []
                grouped_by_timestamp[timestamp].append({'action_type': entry['action_type'], 'details': details, 'user_email': user_email})
            # sort 'data_update' actions by timestamp from most recent to oldesr
            # these should be called sorted_du
            sorted_du = sorted(
                [(timestamp, details) for timestamp, details in grouped_by_timestamp.items() if any(d['action_type'] == 'DATA_UPDATE' for d in details)],
                key=lambda x: x[0],
                reverse=True
            )
            current_data = Data.objects.filter(indicator=indicator).order_by('period')
            data_history = {}
            # loop through all timestamps in sorted_du
            for timestamp, details in sorted_du:
                time_list = [{} for _ in range(len(current_data))]
                for index, data in enumerate(current_data):
                    period = data.period
                    value = data.value
                    if sorted_du.index((timestamp, details)) == 0:
                        time_list[index] = {'period': period, 'value': value}
                    else:
                        # get previous iteration's timestamp
                        previous_timestamp = sorted_du[sorted_du.index((timestamp, details)) - 1][0]
                        # get data history in position previous iteration timestamp for given period
                        previous_value = data_history.get(str(previous_timestamp), [])[index]['value']
                        # if previous value contains '->' then get what is after '->' and set it as the new value
                        if '->' in str(previous_value):
                            previous_value = previous_value.split('->')[0].strip()
                        time_list[index] = {'period': period, 'value': previous_value}
                    for detail in details:
                        user = detail['user_email']
                        for change in detail['details']:
                            if change['period'] == period:
                                old_value = change['old_value']
                                new_value = change['new_value']
                                time_list[index] = {'period': period, 'value': f'{old_value} -> {new_value}', 'user': user}
                data_history[str(timestamp)] = time_list
            response = [{'timestamp': timestamp, 'details': details_list} for timestamp, details_list in grouped_by_timestamp.items() if not any(d['action_type'] == 'DATA_UPDATE' for d in details_list)]
            response.append({'action_type': 'DATA_UPDATE', 'history': data_history})
            return JsonResponse(response, safe=False)
    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)


def add_indicators_to_table(request, id):
    try:
        if request.method == 'POST':
            selected_indicators = json.loads(request.body)
            table = CustomTable.objects.get(id=id)
            if not table:
                return JsonResponse({'error': f'Table with id {id} not found'}, status=404)
            # check if selected indicators are already in the table
            for indicator_id in selected_indicators:
                indicator = Indicator.objects.get(id=indicator_id)
                if not indicator:
                    return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)
                # check whether frequency of indicator matches the frequency of other indicators in the table
                # select one indicator from the table to compare with
                if table.indicators.all():
                    first_indicator = table.indicators.first()
                    if first_indicator.frequency != indicator.frequency:
                        return JsonResponse({'error': f'Indicator with id {indicator_id} has different data frequency than other indicators in the table'}, status=422)
                    if indicator not in table.indicators.all():
                        table.indicators.add(indicator)
                else:
                    table.indicators.add(indicator)
            return JsonResponse({'success': f'Indicators {selected_indicators} successfully'})

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)})

def indicators(request, id):
    try:
        if request.method == 'GET':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            indicator = Indicator.objects.get(id=id)

            # Check view permission - Django's built-in permission will be used here
            if not check_indicator_permission(user, indicator, 'view'):
                return JsonResponse({'error': 'Permission denied'}, status=403)

            # get all Data entries for this Indicator
            data_qs = Data.objects.filter(indicator=indicator)

            # Group data by period => single 'value' per period
            data_by_period = {}
            for d in data_qs:
                # Store the value and id.
                data_by_period[d.period] = {'value': d.value, 'id': d.id}

            # Sort by period (ascending) and build a list of rows
            data_list = []
            for period, val in sorted(data_by_period.items()):
                data_list.append({
                    'period': period,
                    'value': val['value'],
                    'id': val['id']
                })


            # get indicator region:
            region = indicator.region.name if indicator.region else None
            country = indicator.country.name if indicator.country else None
            can_edit = check_indicator_permission(user, indicator, 'edit')

            try:
                # Build metadata for the Indicator (including its unit)
                indicator_metadata = {
                    'name': indicator.name,
                    'region': region,
                    'country': country,
                    'code': indicator.code,
                    'base_year': indicator.base_year,
                    'description': indicator.description,
                    'source': indicator.source,
                    'category': indicator.category.name if indicator.category else None,
                    'is_seasonally_adjusted': indicator.seasonally_adjusted,
                    'frequency': indicator.frequency,
                    'other_frequency': indicator.other_frequency,
                    'is_custom': indicator.is_custom,
                    'currentPrices': indicator.currentPrices,
                    'unit': indicator.unit.name if indicator.unit else None,
                    'can_edit': can_edit,
                    'access_level': indicator.access_level.level,
                }
            except Exception as e:
                print(f"Error building indicator metadata: {e}")
                return JsonResponse({'error': str(e)}, status=500)

            if indicator.is_custom:
                custom_indicator = CustomIndicator.objects.get(indicator=indicator)
                check_custom_indicator_permission(user, custom_indicator, 'view')
                if custom_indicator:
                    basis_indicators = custom_indicator.base_indicators.all()
                    basis_indicator_metadata = {}
                    basis_indicator_data = {}
                    for basis_indicator in basis_indicators:
                        region = basis_indicator.region.name if basis_indicator.region else None
                        country = basis_indicator.country.name if basis_indicator.country else None
                        try:
                            basis_indicator_metadata[basis_indicator.code] = {
                                'id': basis_indicator.id,
                                'name': basis_indicator.name,
                                'region': region,
                                'country': country,
                                'code': basis_indicator.code,
                                'base_year': basis_indicator.base_year,
                                'description': basis_indicator.description,
                                'source': basis_indicator.source,
                                'category': basis_indicator.category.name if basis_indicator.category else None,
                                'is_seasonally_adjusted': basis_indicator.seasonally_adjusted,
                                'frequency': basis_indicator.frequency,
                                'other_frequency': basis_indicator.other_frequency,
                                'is_custom': basis_indicator.is_custom,
                                'currentPrices': basis_indicator.currentPrices,
                                'unit': basis_indicator.unit.name if basis_indicator.unit else None  # <-- new
                            }
                        except Exception as e:
                            print(f"Error processing basis indicator {basis_indicator.name}: {e}")
                        data_qs = Data.objects.filter(indicator=basis_indicator)

                        # Group data by period => single 'value' per period
                        data_by_period = {}
                        for d in data_qs:
                            # Store the value and id.
                            data_by_period[d.period] = {'value': d.value, 'id': d.id}


                        # Sort by period (ascending) and build a list of rows
                        data_l = []
                        for period, val in sorted(data_by_period.items()):
                            data_l.append({
                                'period': period,
                                'value': val['value'],
                                'id': val['id']
                            })
                        basis_indicator_data[basis_indicator.code] = data_l
                    formula = custom_indicator.formula
                    return JsonResponse({'indicator':indicator_metadata, 'data': data_list, 'basis_indicators': basis_indicator_metadata, 'basis_data': basis_indicator_data, 'formula': formula})



            return JsonResponse({'indicator': indicator_metadata, 'data': data_list})

        elif request.method == 'POST':  # Edit
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            indicator = Indicator.objects.get(id=id)

            # Check edit permission - uses custom 'edit_indicator' permission
            if not check_indicator_permission(user, indicator, 'edit'):
                return JsonResponse({'error': 'Permission denied'}, status=403)

            old_indicator = Indicator.objects.get(id=id)
            old_data = {
                'name': old_indicator.name,
                'code': old_indicator.code,
                'description': old_indicator.description,
                'base_year': old_indicator.base_year,
                'source': old_indicator.source,
                'seasonally_adjusted': old_indicator.seasonally_adjusted,
                'frequency': old_indicator.frequency,
                'other_frequency': old_indicator.other_frequency,
                'currentPrices': old_indicator.currentPrices,
                'category': getattr(old_indicator.category, 'name', None),
                'country': getattr(old_indicator.country, 'name', None),
                'region': getattr(old_indicator.region, 'name', None),
                'unit': getattr(old_indicator.unit, 'name', None),
            }
            other_frequency=None
            data = json.loads(request.body)
            indicator = Indicator.objects.get(id=id)
            if not indicator:
                return JsonResponse({'error': f'Indicator with id {id} not found'}, status=404)

            print(data)
            location_type = data.get('location_type')
            name = data.get('name', indicator.name)
            code = data.get('code', indicator.code)
            description = data.get('description', indicator.description)
            base_year = data.get('base_year', indicator.base_year)
            seasonally_adjusted = data.get('seasonally_adjusted', indicator.seasonally_adjusted)
            frequency = data.get('frequency', indicator.frequency)
            if frequency == 'CUSTOM':
                other_frequency = data.get('other_frequency', None)
            is_custom = data.get('is_custom', indicator.is_custom)
            current_prices = data.get('current_prices', indicator.currentPrices)
            source = data.get('source')
            category_name = data.get('category')
            country_name = data.get('country')
            region_name = data.get('region')
            unit_name = data.get('unit')


            category = Category.objects.filter(name=category_name).first() if category_name else indicator.category
            country = Country.objects.filter(name=country_name).first() if country_name else indicator.country
            region = Region.objects.filter(name=region_name).first() if region_name else indicator.region
            unit = Unit.objects.filter(name=unit_name).first() if unit_name else indicator.unit


            indicator.name = name
            indicator.code = code
            indicator.description = description
            indicator.base_year = base_year
            indicator.seasonally_adjusted = (seasonally_adjusted == 'true')
            indicator.frequency = frequency
            indicator.is_custom = (is_custom == 'true')
            indicator.currentPrices = (current_prices == 'true')
            indicator.category = category
            indicator.unit = unit
            indicator.source = source
            indicator.other_frequency = other_frequency

            # Decide how to handle country/region
            if location_type == "country":
                indicator.country = country
                indicator.region = None
            elif location_type == "region":
                indicator.region = region
                indicator.country = None

            indicator.save()

            new_indicator = Indicator.objects.get(id=id)
            changes = {}

            # Compare simple fields
            simple_fields = [
                'name', 'code', 'description', 'base_year',
                'seasonally_adjusted', 'frequency', 'currentPrices'
            ]

            for field in simple_fields:
                old_val = old_data.get(field)
                new_val = getattr(new_indicator, field)
                if old_val != new_val:
                    changes[field] = {'old': old_val, 'new': new_val}

            # Compare relationships
            relationship_fields = ['category', 'country', 'region', 'unit']
            for field in relationship_fields:
                old_val = old_data.get(field)
                new = getattr(new_indicator, field)
                if new:
                    new_val = new.name
                else:
                    new_val = None

                if old_val != new_val:
                    changes[field] = {'old': old_val, 'new': new_val}
            print("user",get_user(request))

            # Create log entry if any changes
            if changes:
                ActionLog.objects.create(
                    user=get_user(request),
                    indicator=new_indicator,
                    action_type='INDICATOR_EDIT',
                    details=changes
                )

            return JsonResponse({'success': 'Indicator updated successfully', 'indicator_id': indicator.id})

        elif request.method == 'DELETE':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            indicator = Indicator.objects.get(id=id)

            # Check delete permission - Django's built-in permission will be used here
            if not check_indicator_permission(user, indicator, 'delete'):
                return JsonResponse({'error': 'Permission denied'}, status=403)

            # Check if any custom indicators depend on this one
            dependent_indicators = CustomIndicator.objects.filter(base_indicators=indicator)
            if dependent_indicators.exists():
                return JsonResponse({
                    'error': f'Cannot delete: This indicator is used by {dependent_indicators.count()} custom indicators'
                }, status=400)

            indicator.delete()
            return JsonResponse({'success': 'Indicator deleted successfully'}, status=204)

        # If none of the above:
        return JsonResponse({'error': 'Invalid request method.'}, status=400)

    except Exception as e:
        print(e)
        return JsonResponse({'error rendering indicator': str(e)}, status=500)



def update_dependent_custom_indicators(updated_indicator,user):
    """
    Recalculate values for Custom Indicators that depend on the updated base indicator.
    """
    print(f"Checking for dependent custom indicators on {updated_indicator.name}")

    # Find all custom indicators that use this indicator as a base
    dependent_custom_indicators = CustomIndicator.objects.filter(base_indicators=updated_indicator)

    for custom_indicator in dependent_custom_indicators:
        print(f"Recomputing values for Custom Indicator: {custom_indicator.indicator.name}")

        # Retrieve all unique periods associated with the updated base indicator
        periods = Data.objects.filter(indicator=updated_indicator).values_list('period', flat=True).distinct()
        changes = []
        for period in periods:
            old_value = Data.objects.filter(indicator=custom_indicator.indicator, period=period).first()
            old_value = old_value.value if old_value else None
            computed_value = custom_indicator.calculate_value(period)

            if old_value is None or f"{old_value:.5f}" != f"{float(computed_value):.5f}":
                data_entries = Data.objects.filter(
                    indicator=custom_indicator.indicator,
                    period=period
                )

                if data_entries.exists():
                    data_entries.update(value=computed_value)
                else:
                    Data.objects.create(
                        indicator=custom_indicator.indicator,
                        period=period,
                        value=computed_value
                    )
                changes.append({'period': period, 'old_value': str(old_value), 'new_value': str(computed_value)})

            print(period,computed_value)
        if changes:
            ActionLog.objects.create(
                user=user,
                indicator=custom_indicator.indicator,
                action_type='DATA_UPDATE',
                details=changes
            )

        print(f"Updated values for {custom_indicator.indicator.name}")


def data(request, indicator_id):
    if request.method == 'POST':
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)

        try:
            indicator = Indicator.objects.get(id=indicator_id)

            # Check edit permission
            if not check_indicator_permission(user, indicator, 'edit'):
                return JsonResponse({'error': 'Permission denied to edit this indicator'}, status=403)

            # Continue with existing logic...

        except Indicator.DoesNotExist:
            return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)

    # ...rest of the function...
        data = json.loads(request.body)
        try:
            indicator = Indicator.objects.get(id=indicator_id)
        except Indicator.DoesNotExist:
            return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)

        print(f"Updating data for Indicator: {indicator.name}")
        changes =[]
        user = get_user(request)

        with transaction.atomic():
            for entry in data:
                period = entry.get('period')
                value = entry.get('value')
                id = entry.get('id')
                is_estimate = entry.get('is_estimate', False)
                print(entry)
                try:
                    if id:
                        data_obj = Data.objects.get(id=id)
                        old_value = data_obj.value
                        # Check if old_value is None before formatting
                        old_value_formatted = f"{float(old_value):.5f}" if old_value is not None else None
                        new_value_formatted = f"{float(value):.5f}" if value is not None else None

                        if old_value_formatted is None or new_value_formatted is None or old_value_formatted != new_value_formatted:
                            changes.append({'period': period, 'data_id': id, 'old_value': str(old_value) if old_value is not None else "None", 'new_value': str(value) if value is not None else "None"})
                        print(f"Updating data point: {id}")
                        # Convert value to float only if it's not None
                        float_value = float(value) if value is not None else None
                        Data.objects.filter(id=id).update(period=period, value=float_value, isEstimate=is_estimate)
                    else:
                        Data.objects.create(indicator=indicator, period=period, value=value, isEstimate=is_estimate)
                        changes.append({'period': period, 'data_id':id, 'old_value': 'None', 'new_value': str(value)})
                except Exception as e:
                    print(e)
                    return JsonResponse({'error': str(e)}, status=500)
        if changes:
            ActionLog.objects.create(
                user=user,
                indicator=indicator,
                action_type='DATA_UPDATE',
                details=changes
            )
        # **Trigger Recalculation for Dependent Custom Indicators**
        update_dependent_custom_indicators(indicator,user)

        return JsonResponse({'success': 'Data points updated successfully'}, status=200)


def restore_indicator_data(request, indicator_id):
    """
    Restore indicator data to values from a specific timestamp in history.
    Uses the exact history entries from the frontend to ensure accuracy.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'Authentication required'}, status=401)

        indicator = Indicator.objects.get(id=indicator_id)
        if not check_indicator_permission(user, indicator, 'edit'):
            return JsonResponse({'error': 'Permission denied to edit this indicator'}, status=403)

        data = json.loads(request.body)
        # print(f'data: {data}')
        timestamp = data.get('timestamp')
        restore_type = data.get('type')  # 'original' or 'changed'
        history_entries = data.get('entries', [])
        print(f'hisstory:{history_entries}')

        if not history_entries:
            return JsonResponse({'error': 'No data entries provided for restoration'}, status=400)

        changes = []
        with transaction.atomic():
            for entry in history_entries:
                period = entry.get('period')
                value_string = entry.get('value', '')

                # Skip entries without period or value
                if not period or not value_string:
                    continue

                # Handle value format based on entry type
                if '->' in value_string:
                    old_value, new_value = value_string.split('->')
                    old_value = old_value.strip()
                    new_value = new_value.strip()

                    # Select value based on restore type
                    value_to_restore = old_value if restore_type == 'original' else new_value
                else:
                    # For entries without changes, just use the value as is
                    value_to_restore = value_string.strip()

                # Skip if value is "None"
                if value_to_restore == "None":
                    float_value = None
                else:

                    try:
                        float_value = float(value_to_restore)
                    except (ValueError, TypeError):
                        float_value = None

                # Find existing data point or create new one
                data_obj = Data.objects.filter(indicator=indicator, period=period).last()

                if data_obj:
                    # Update existing data point if value changed
                    if float_value is None:
                        # If the value is None, check if the existing value is not None
                        if data_obj.value is not None:
                            old_value = str(data_obj.value) if data_obj.value is not None else "None"
                            changes.append({
                                'period': period,
                                'old_value': old_value,
                                'new_value': 'None'
                            })
                            data_obj.value = None
                            data_obj.save()
                    elif data_obj.value is None or abs(float(data_obj.value) - float_value) > 0.00001:
                        old_value = str(data_obj.value) if data_obj.value is not None else "None"
                        changes.append({
                            'period': period,
                            'old_value': old_value,
                            'new_value': str(float_value)
                        })
                        data_obj.value = float_value
                        data_obj.save()
                else:
                    # Create new data point
                    Data.objects.create(
                        indicator=indicator,
                        period=period,
                        value=float_value
                    )
                    changes.append({
                        'period': period,
                        'old_value': 'None',
                        'new_value': str(float_value)
                    })

            if changes:
                # Log the restoration action
                ActionLog.objects.create(
                    user=user,
                    indicator=indicator,
                    action_type='DATA_UPDATE',
                    details=changes
                )



                # Update dependent indicators
                update_dependent_custom_indicators(indicator, user)

                return JsonResponse({
                    'success': True,
                    'message': f'Successfully restored {len(changes)} data points to {restore_type} values',
                    'changes_count': len(changes)
                })
            else:
                return JsonResponse({'success': True, 'message': 'No changes needed'})

    except Exception as e:
        print(f"Error in restore_indicator_data: {e}")
        return JsonResponse({'error': str(e)}, status=500)


def create_custom_indicator(request, indicator_id):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=400)

    try:
        with transaction.atomic():
            # Retrieve the base indicator
            data = json.loads(request.body)
            formula = data.get('formula')
            formula = formula.replace('^', '**')
            indicator = Indicator.objects.get(id=indicator_id)
            print(f'applying {formula} to {indicator.name}')
            old_formula = None
            try:
                existing = CustomIndicator.objects.get(indicator=indicator)
                old_formula = existing.formula
            except CustomIndicator.DoesNotExist:
                pass
            # Identify base indicators used in the formula
            base_indicators = []
            for token in formula.split():  # Simple token parsing
                if token.startswith('@'):
                    indicator_code = token[1:]  # Remove '@'
                    base_indicator = Indicator.objects.filter(code=indicator_code).first()
                    if base_indicator:
                        base_indicators.append(base_indicator)
                    else:
                        raise ValueError(f"Base indicator with code '{indicator_code}' not found.")

            # Check if CustomIndicator already exists for the given indicator
            custom_indicator, created = CustomIndicator.objects.update_or_create(
                indicator=indicator,
                defaults={'formula': formula}
            )
            custom_indicator.base_indicators.set(base_indicators)

            ActionLog.objects.create(
                user=get_user(request),
                indicator=indicator,
                action_type='FORMULA_UPDATE',
                details={
                    'old_formula': old_formula,
                    'new_formula': formula,
                    'base_indicators': [i.code for i in base_indicators]
                }
            )


            # Retrieve all unique periods from base indicators
            periods = Data.objects.filter(indicator__in=base_indicators).values_list('period', flat=True).distinct()

            changes = []
            # Generate calculated values for the new custom indicator
            for period in periods:
                computed_value = custom_indicator.calculate_value(period)
                if computed_value is not None:
                    data_entry, created = Data.objects.get_or_create(
                        indicator=indicator,
                        period=period,
                        defaults={'value': computed_value}
                    )
                    if not created:
                        old_value = data_entry.value
                        if old_value is None or f"{old_value:.5f}" != f"{float(computed_value):.5f}":
                            changes.append({'period': period, 'old_value': str(old_value), 'new_value': str(computed_value)})
                            data_entry.value = computed_value
                            data_entry.save()

            if changes:
                ActionLog.objects.create(
                    user=get_user(request),
                    indicator=indicator,
                    action_type='DATA_UPDATE',
                    details=changes
                )



            return JsonResponse({"message": "Custom indicator created successfully"}, status=201)
    except Exception as e:
        print(f"Error creating custom indicator: {e}")
        return JsonResponse({"error": str(e)}, status=500)

# Add a new endpoint for managing permissions
def manage_indicator_permissions(request, indicator_id):
    try:
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)

        indicator = Indicator.objects.get(id=indicator_id)


        if request.method == 'GET':
            # Only indicator owners or admins can modify permissions
            if not (user.is_superuser or check_indicator_permission(user, indicator, 'view')):
                return JsonResponse({'error': 'Permission denied'}, status=403)

            # Return current permissions
            try:
                access_level = indicator.access_level.level
                access_level_display = indicator.access_level.get_level_display()
            except:
                access_level = AccessLevel.PUBLIC
                access_level_display = 'Public Viewing'
                initialize_indicator_access(indicator)

            # Get user-specific permissions for restricted indicators
            user_permissions = []
            if access_level == AccessLevel.RESTRICTED:
                perms = IndicatorPermission.objects.filter(indicator=indicator).select_related('user')
                for perm in perms:
                    user_permissions.append({
                        'user_id': perm.user.id,
                        'email': perm.user.email,
                        'first_name': perm.user.first_name,
                        'last_name': perm.user.last_name,
                        'can_view': perm.can_view,
                        'can_edit': perm.can_edit,
                        'can_delete': perm.can_delete
                    })

            return JsonResponse({
                'access_level': access_level,
                'access_level_display': access_level_display,
                'user_permissions': user_permissions
            })

        elif request.method == 'POST':
            if not (user.is_superuser or check_indicator_permission(user, indicator, 'edit')):
                return JsonResponse({'error': 'Permission denied'}, status=403)
            # Update permissions
            data = json.loads(request.body)
            new_level = data.get('access_level')
            user_permissions = data.get('user_permissions', [])

            with transaction.atomic():
                try:
                    access = indicator.access_level
                    access.level = new_level
                    access.save()
                except:
                    AccessLevel.objects.create(indicator=indicator, level=new_level)

                # Clear existing permissions
                if new_level == AccessLevel.RESTRICTED:
                    IndicatorPermission.objects.filter(indicator=indicator).delete()

                    # Add new permissions
                    for perm in user_permissions:
                        IndicatorPermission.objects.create(
                            user_id=perm['user_id'],
                            indicator=indicator,
                            can_view=perm.get('can_view', False),
                            can_edit=perm.get('can_edit', False),
                            can_delete=perm.get('can_delete', False)
                        )
                else:
                    # If not restricted, remove all user-specific permissions
                    IndicatorPermission.objects.filter(indicator=indicator).delete()

            return JsonResponse({'success': 'Permissions updated'})

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

# Add a function to get all users for permission assignment
def get_users(request):
    try:
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)

        users = UserAccount.objects.values('id', 'email', 'first_name', 'last_name')
        return JsonResponse(list(users), safe=False)

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

# Add a function to get user activity history
def user_activity(request, user_id):
    try:
        if request.method == 'GET':
            # Ensure the requesting user is authenticated
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)


            # Get the user whose activity we're showing
            try:
                target_user = UserAccount.objects.get(id=user_id)
            except UserAccount.DoesNotExist:
                return JsonResponse({'error': f'User with id {user_id} not found'}, status=404)

            # Get all action logs for this user
            action_logs = ActionLog.objects.filter(user=target_user).order_by('-timestamp')

            # Group by indicators and action types
            grouped_activity = {}

            for log in action_logs:
                if user.is_superuser or check_indicator_permission(user, log.indicator, 'view'):
                    indicator_id = log.indicator.id
                    indicator_name = log.indicator.name
                    indicator_code = log.indicator.code

                    if indicator_id not in grouped_activity:
                        grouped_activity[indicator_id] = {
                            'indicator_name': indicator_name,
                            'indicator_code': indicator_code,
                            'actions': []
                        }

                    # Format the timestamp
                    formatted_time = log.timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    # Process different types of actions
                    action_details = {
                        'action_type': log.action_type,
                        'timestamp': formatted_time,
                        'details': log.details
                    }

                    # Add specific processing for different action types if needed
                    if log.action_type == 'DATA_UPDATE':
                        # Count the number of data points changed
                        if isinstance(log.details, list):
                            action_details['points_changed'] = len(log.details)

                    grouped_activity[indicator_id]['actions'].append(action_details)

            # Convert to a format better for frontend rendering
            activity_list = []
            for indicator_id, data in grouped_activity.items():
                activity_list.append({
                    'indicator_id': indicator_id,
                    'indicator_name': data['indicator_name'],
                    'indicator_code': data['indicator_code'],
                    'actions': data['actions']
                })

            # Sort by most recent activity
            activity_list.sort(key=lambda x: max(action['timestamp'] for action in x['actions']), reverse=True)

            return JsonResponse({
                'user': {
                    'id': target_user.id,
                    'email': target_user.email,
                    'first_name': target_user.first_name,
                    'last_name': target_user.last_name
                },
                'activity': activity_list
            })

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def follow_user(request, user_id):
    try:
        if request.method == 'POST':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            target_user = UserAccount.objects.get(id=user_id)
            if not target_user:
                return JsonResponse({'error': f'User with id {user_id} not found'}, status=404)

            follow_relation, created = UserFollowsUser.objects.get_or_create(user=user)
            follow_relation.followed_users.add(target_user)
            return JsonResponse({'success': f'User {user_id} followed successfully'}, status=200)

        elif request.method == 'DELETE':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            target_user = UserAccount.objects.get(id=user_id)
            if not target_user:
                return JsonResponse({'error': f'User with id {user_id} not found'}, status=404)

            follow_relation = UserFollowsUser.objects.get(user=user)
            follow_relation.followed_users.remove(target_user)
            return JsonResponse({'success': f'User {user_id} unfollowed successfully'}, status=200)

        else:
            return JsonResponse({'error': 'Invalid request method'}, status=400)

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

# Add a new function to get users the current user is following
def get_user_following(request):
    """
    Get all users that the current user is following
    """
    try:
        user = get_user(request)
        if not user:
            return JsonResponse({'error': 'User not authenticated'}, status=401)

        # Get the user's follows record
        follow_relation, created = UserFollowsUser.objects.get_or_create(user=user)

        # Get all users being followed
        followed_users = follow_relation.followed_users.all()
        user_list = []

        for followed_user in followed_users:
            user_list.append({
                'id': followed_user.id,
                'email': followed_user.email,
                'first_name': followed_user.first_name,
                'last_name': followed_user.last_name,
                'is_superuser': followed_user.is_superuser,
            })

        return JsonResponse(user_list, safe=False)

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def favourite_table(request, table_id):
    try:
        if request.method == 'POST':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            table = CustomTable.objects.get(id=table_id)
            if not table:
                return JsonResponse({'error': f'Table with id {table_id} not found'}, status=404)

            favourite_relation, created = UserFavouriteTables.objects.get_or_create(user=user)
            favourite_relation.tables.add(table)
            return JsonResponse({'success': f'Table {table_id} added to favourites'}, status=200)

        elif request.method == 'DELETE':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            table = CustomTable.objects.get(id=table_id)
            if not table:
                return JsonResponse({'error': f'Table with id {table_id} not found'}, status=404)

            favourite_relation = UserFavouriteTables.objects.get(user=user)
            favourite_relation.tables.remove(table)
            return JsonResponse({'success': f'Table {table_id} removed from favourites'}, status=200)

        else:
            return JsonResponse({'error': 'Invalid request method'}, status=400)

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)

def favourite_indicator(request, indicator_id):
    try:
        if request.method == 'POST':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            indicator = Indicator.objects.get(id=indicator_id)
            if not indicator:
                return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)

            favourite_relation, created = UserFavouriteIndicators.objects.get_or_create(user=user)
            favourite_relation.indicators.add(indicator)
            return JsonResponse({'success': f'Indicator {indicator_id} added to favourites'}, status=200)

        elif request.method == 'DELETE':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            indicator = Indicator.objects.get(id=indicator_id)
            if not indicator:
                return JsonResponse({'error': f'Indicator with id {indicator_id} not found'}, status=404)

            favourite_relation = UserFavouriteIndicators.objects.get(user=user)
            favourite_relation.indicators.remove(indicator)
            return JsonResponse({'success': f'Indicator {indicator_id} removed from favourites'}, status=200)

        else:
            return JsonResponse({'error': 'Invalid request method'}, status=400)

    except Exception as e:
        print(e)
        return JsonResponse({'error': str(e)}, status=500)


def followed_user_activity(request):
    if request.method == 'GET':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            # Get pagination parameters
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 10))

            followed_users = UserFollowsUser.objects.get(user=user).followed_users.all()
            activity = []
            for followed_user in followed_users:
                action_logs = ActionLog.objects.filter(user=followed_user)
                for log in action_logs:
                    if check_indicator_permission(user, log.indicator, 'view'):
                        activity_item = {
                            'user': followed_user.email,
                            'action': log.action_type,
                            'indicator': log.indicator.name,
                            'indicator_id': log.indicator.id,
                            'details': log.details,
                            'timestamp': log.timestamp
                        }
                        activity.append(activity_item)

            # Sort by timestamp, newest first
            sorted_activity = sorted(activity, key=lambda x: x['timestamp'], reverse=True)

            # Apply pagination
            total_items = len(sorted_activity)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_results = sorted_activity[start_idx:end_idx]
            has_more = end_idx < total_items

            return JsonResponse({
                'results': paginated_results,
                'has_more': has_more,
                'total': total_items,
                'page': page,
                'page_size': page_size
            }, safe=False)
        except Exception as e:
            print(e)
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=400)

def favourite_indicator_activity(request):
    if request.method == 'GET':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            # Get pagination parameters
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 10))
            activity_type = request.GET.get('type', 'all')  # 'all', 'info', or 'data'

            try:
                favourite_indicators = UserFavouriteIndicators.objects.get(user=user).indicators.all()
            except UserFavouriteIndicators.DoesNotExist:
                return JsonResponse({
                    'info': [],
                    'data_changes': [],
                    'has_more_info': False,
                    'has_more_data': False,
                    'total_info': 0,
                    'total_data': 0
                }, safe=False)

            info = []
            data_changes = []
            for indicator in favourite_indicators:
                action_logs = ActionLog.objects.filter(indicator=indicator)
                for log in action_logs:
                    if not check_indicator_permission(user, log.indicator, 'view'):
                        continue
                    try:
                        if log.action_type == 'INDICATOR_CREATE':
                            info.append({
                                'type': 'CREATED',
                                'indicator_id': log.indicator.id,
                                'indicator': log.indicator.name,
                                'details': log.details,
                                'timestamp': log.timestamp,
                            })
                        elif log.action_type == 'DATA_UPDATE':
                            for detail in log.details:
                                if detail.get('old_value') == 'None':
                                    indicator_data_sorted = Data.objects.filter(indicator=indicator).order_by('period')
                                    period_of_change = detail.get('period')
                                    value_of_change = detail.get('new_value')
                                    previous_period_data = indicator_data_sorted.filter(period__lt=period_of_change).last()
                                    if previous_period_data:
                                        try:
                                            prev_value = float(previous_period_data.value)
                                            if prev_value == 0:
                                                # Handle division by zero
                                                percentage_change = None
                                            else:
                                                percentage_change = ((float(value_of_change) - prev_value) / prev_value) * 100
                                        except (ValueError, TypeError, ZeroDivisionError):
                                            percentage_change = None

                                        data_changes.append({
                                            'type': 'CREATED',
                                            'indicator_id': log.indicator.id,
                                            'indicator': log.indicator.name,
                                            'timestamp': log.timestamp,
                                            'percentage_change': percentage_change,
                                            'current_period': period_of_change,
                                            'previous_period': previous_period_data.period,
                                            'current_value': value_of_change,
                                            'previous_value': previous_period_data.value,
                                        })
                                    else:
                                        data_changes.append({
                                            'type': 'CREATED',
                                            'indicator_id': log.indicator.id,
                                            'indicator': log.indicator.name,
                                            'timestamp': log.timestamp,
                                            'current_period': period_of_change,
                                            'current_value': value_of_change,
                                            'previous_period': None,
                                            'previous_value': None,
                                            'percentage_change': None,
                                        })
                                else:
                                    try:
                                        previous_value = float(detail.get('old_value'))
                                        new_value = float(detail.get('new_value'))
                                        if previous_value == 0:
                                            # Handle division by zero
                                            percentage_change = None
                                        else:
                                            percentage_change = ((new_value - previous_value) / previous_value) * 100
                                    except (ValueError, TypeError, ZeroDivisionError):
                                        percentage_change = None

                                    data_changes.append({
                                        'type': 'UPDATED',
                                        'indicator_id': log.indicator.id,
                                        'indicator': log.indicator.name,
                                        'details': log.details,
                                        'period': detail.get('period'),
                                        'percentage_change': percentage_change,
                                        'timestamp': log.timestamp,
                                    })
                            else:
                                try:
                                    previous_value = float(log.details[0].get('old_value'))
                                    new_value = float(log.details[0].get('new_value'))
                                    if previous_value == 0:
                                        # Handle division by zero
                                        percentage_change = None
                                    else:
                                        percentage_change = ((new_value - previous_value) / previous_value) * 100
                                except (ValueError, TypeError, ZeroDivisionError, IndexError):
                                    percentage_change = None

                                data_changes.append({
                                    'type': 'UPDATED',
                                    'indicator_id': log.indicator.id,
                                    'indicator': log.indicator.name,
                                    'details': log.details,
                                    'period': log.details[0].get('period') if log.details and len(log.details) > 0 else None,
                                    'percentage_change': percentage_change,
                                    'timestamp': log.timestamp,
                                })
                        elif log.action_type == 'INDICATOR_EDIT':
                            info.append({
                                'type': 'EDITED',
                                'indicator_id': log.indicator.id,
                                'indicator': log.indicator.name,
                                'details': log.details,
                                'timestamp': log.timestamp,
                            })
                        elif log.action_type == 'FORMULA_UPDATE':
                            if log.details.get('old_formula') == 'None':
                                data_changes.append({
                                    'type': 'CREATED FORMULA',
                                    'indicator_id': log.indicator.id,
                                    'indicator': log.indicator.name,
                                    'formula': log.details.get('new_formula'),
                                    'timestamp': log.timestamp,
                                })
                            else:
                                data_changes.append({
                                    'type': 'CHANGED FORMULA',
                                    'indicator_id': log.indicator.id,
                                    'indicator': log.indicator.name,
                                    'details': log.details,
                                    'timestamp': log.timestamp,
                                })
                    except (KeyError, ValueError, TypeError) as e:
                        print(f"Skipping log due to error: {e}")
                        continue

            # First sort everything by timestamp (most recent first)
            all_info = sorted(info, key=lambda x: x['timestamp'], reverse=True)
            all_data_changes = sorted(data_changes, key=lambda x: x['timestamp'], reverse=True)

            # Total counts for pagination
            total_info = len(all_info)
            total_data = len(all_data_changes)

            # Apply pagination and activity type filtering
            start_idx_info = (page - 1) * page_size
            end_idx_info = start_idx_info + page_size
            start_idx_data = (page - 1) * page_size
            end_idx_data = start_idx_data + page_size

            if activity_type == 'all' or activity_type == 'info':
                paginated_info = all_info[start_idx_info:end_idx_info]
                has_more_info = end_idx_info < total_info
            else:
                paginated_info = []
                has_more_info = False

            if activity_type == 'all' or activity_type == 'data':
                paginated_data_changes = all_data_changes[start_idx_data:end_idx_data]
                has_more_data = end_idx_data < total_data
            else:
                paginated_data_changes = []
                has_more_data = False

            return JsonResponse({
                'info': paginated_info,
                'data_changes': paginated_data_changes,
                'has_more_info': has_more_info,
                'has_more_data': has_more_data,
                'total_info': total_info,
                'total_data': total_data,
                'page': page,
                'page_size': page_size
            }, safe=False)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=400)
