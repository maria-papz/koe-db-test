import decimal
import requests
from celery import shared_task
from django_celery_beat.models import PeriodicTask, CrontabSchedule
import json
from koe_db.models import EuroStatRequest, Workflow, CyStatRequest, CyStatIndicatorMapping, Data, Indicator, ActionLog, WorkflowRun, ECBRequest, EuroStatIndicatorMapping
from django.utils import timezone
from django.db import transaction
from decimal import Decimal
from koe_db.api_views import update_dependent_custom_indicators

@shared_task
def execute_cystat_request(cystat_request_id):
    """
    Executes a CyStat request and prints the period, indicator, and value.
    """
    workflow_run = None
    try:
        cystat_request = CyStatRequest.objects.get(id=cystat_request_id)
        workflow = cystat_request.workflow
        # Create a WorkflowRun
        workflow_run = WorkflowRun.objects.create(
            workflow=workflow,
            start_time=timezone.now(),
            status="RUNNING",
            success=False
        )

        print(f"Executing CyStat request for workflow: {cystat_request.workflow.name}")

        # Fetch the request body and URL
        url = cystat_request.url
        request_body = cystat_request.request_body

        # Perform a GET request to extract variables and periods
        try:
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
        except Exception as e:
            print(f"Failed to fetch structure from {url}: {e}")
            workflow_run.status = "FAILED"
            workflow_run.error_message=f"Failed to fetch structure from {url}: {e}"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.save()
            return

        # Extract variables and periods
        variables = json_data.get("variables", [])
        title = json_data.get("title", "")
        period_index = None
        periods_array = []

        for index, variable in enumerate(variables):
            if variable.get("code") in ["QUARTER", "MONTH", "YEAR"]:
                period_index = index
                raw_periods = variable.get("valueTexts", [])
                if variable["code"] == "QUARTER":
                    periods_array = [period[:4] + "-" + period[4:] for period in raw_periods]
                elif variable["code"] == "MONTH":
                    periods_array = [period[:4] + "-" + period[5:].zfill(2) for period in raw_periods]
                elif variable["code"] == "YEAR":
                    periods_array = raw_periods
                break

        if period_index is None:
            print(f"No time-based variable (QUARTER, MONTH, or YEAR) found for {cystat_request.workflow.name}")
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message=f"No time-based variable (QUARTER, MONTH, or YEAR) found for {cystat_request.workflow.name}"
            workflow_run.save()
            return

        # Post the request body to the CyStat API
        try:
            response = requests.post(url, json=request_body)
            response.raise_for_status()
            response_data = response.json()
        except Exception as e:
            print(f"Failed to execute query for {cystat_request.workflow.name}: {e}")
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message=f"Failed to execute query for {cystat_request.workflow.name}: {e}"
            workflow_run.save()
            return

        # Parse the response data
        data = response_data.get("data", [])
        print(f"Processing data for workflow: {cystat_request.workflow.name}...")

        # Match the data to indicators using the mappings
        with transaction.atomic():
            # Create a dictionary to collect changes by indicator
            indicator_changes = {}  # { indicator_id: [change1, change2, ...] }

            mappings = CyStatIndicatorMapping.objects.filter(cystat_request=cystat_request)
            for entry in data:
                # Extract the period using the period index
                period_index_value = int(entry["key"][period_index])
                period = periods_array[period_index_value]

                # Match the entry to an indicator
                for mapping in mappings:
                    indicator = mapping.indicator
                    keys = mapping.key_indices  # Example: {"MEASURE": "1", "TYPE OF DATA": "1", "NA AGGREGATE": "9"}

                    # Construct the expected key array based on variable order
                    expected_keys = []
                    for variable in variables:
                        variable_code = variable["code"]
                        if variable_code in ["QUARTER", "MONTH", "YEAR"]:
                            expected_keys.append("*")  # Wildcard for the period variable
                        else:
                            expected_keys.append(keys.get(variable_code,None))
                            if expected_keys[-1] is None:
                                workflow_run.status = "FAILED"
                                workflow_run.success = False
                                workflow_run.end_time = timezone.now()
                                workflow_run.error_message = f"Variable code '{variable_code}' is no longer compatible. Please update workflow"
                                workflow_run.save()
                                return

                    # Check if the entry matches the indicator's keys
                    match = True
                    for i, key in enumerate(expected_keys):
                        if key == "*":  # Skip wildcard keys
                            continue
                        if entry["key"][i] != key:
                            match = False
                            break

                    if match:
                        # Convert value to Decimal
                        try:
                            print(f"Attempting to convert value: {entry['values'][0]} for period {period} and key {entry['key']}")
                            dec_value = Decimal(str(entry["values"][0]))
                        except (ValueError, KeyError, IndexError, TypeError,decimal.ConversionSyntax, decimal.InvalidOperation):
                            continue

                        # Find or create Data
                        existing_data = Data.objects.filter(
                            indicator=mapping.indicator,
                            period=period
                        ).first()

                        if existing_data:
                            old_val = existing_data.value
                            if old_val != dec_value:
                                existing_data.value = dec_value
                                existing_data.save()

                                # Add to our collection of changes for this indicator
                                change = {
                                    'period': period,
                                    'data_id': existing_data.id,
                                    'old_value': str(old_val),
                                    'new_value': str(dec_value)
                                }
                                indicator_changes.setdefault(mapping.indicator.id, []).append(change)
                        else:
                            new_data = Data.objects.create(
                                indicator=mapping.indicator,
                                period=period,
                                value=dec_value
                            )

                            # Add to our collection of changes for this indicator
                            change = {
                                'period': period,
                                'data_id': new_data.id,
                                'old_value': 'None',
                                'new_value': str(dec_value)
                            }
                            indicator_changes.setdefault(mapping.indicator.id, []).append(change)
                        break

            # Now create one ActionLog per indicator with all its changes
            for indicator_id, changes in indicator_changes.items():
                indicator = Indicator.objects.get(id=indicator_id)

                ActionLog.objects.create(
                    user=None,
                    indicator_id=indicator_id,
                    run=workflow_run,
                    action_type='DATA_UPDATE',
                    details=changes  # This must be a list of change objects for indicator_history
                )
                # Update dependent custom indicators
                if changes:  # Only update if there were actual changes
                    update_dependent_custom_indicators(indicator, None)


            # Mark workflow as completed
            workflow_run.status = "COMPLETED"
            workflow_run.success = True
            workflow_run.end_time = timezone.now()
            workflow_run.save()

            # Update last run time and calculate next run
            workflow.last_run = timezone.now()
            workflow.last_run_success = True
            try:
                from koe_db.workflow_views import calculate_next_run
                workflow.next_run = calculate_next_run(workflow.schedule_cron)
            except Exception as e:
                print(f"Error calculating next run: {str(e)}")
            workflow.save()

    except CyStatRequest.DoesNotExist:
        print(f"CyStatRequest with ID {cystat_request_id} does not exist.")
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message=f"CyStatRequest with ID {cystat_request_id} does not exist."
            workflow_run.save()
    except Exception as e:
        print(f"An error occurred: {e}")
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.error_message = str(e)
            workflow_run.end_time = timezone.now()
            workflow_run.save()


@shared_task
def execute_ecb_request(ecb_request_id):
    """
    Executes an ECB request and updates the corresponding indicator with data.
    """
    workflow_run = None
    try:
        ecb_request = ECBRequest.objects.get(id=ecb_request_id)
        workflow = ecb_request.workflow

        # Create a WorkflowRun
        workflow_run = WorkflowRun.objects.create(
            workflow=workflow,
            start_time=timezone.now(),
            status="RUNNING",
            success=False
        )

        print(f"Executing ECB request for workflow: {ecb_request.workflow.name}")

        # Fetch the indicator for this ECB request
        try:
            indicator = Indicator.objects.get(id=ecb_request.indicator_id)
        except Indicator.DoesNotExist:
            error_msg = f"Indicator with ID {ecb_request.indicator_id} not found for ECB request {ecb_request_id}"
            print(error_msg)
            workflow_run.status = "FAILED"
            workflow_run.error_message = error_msg
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.save()
            return

        # Construct the URL
        url = f"https://data-api.ecb.europa.eu/service/data/{ecb_request.table}/{ecb_request.parameters}?format=jsondata"

        # Fetch data from ECB API
        try:
            response = requests.get(url)
            response.raise_for_status()
            response_data = response.json()
        except Exception as e:
            error_msg = f"Failed to fetch data from ECB API: {str(e)}"
            print(error_msg)
            workflow_run.status = "FAILED"
            workflow_run.error_message = error_msg
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.save()
            return


        # Extract time periods
        periods = []
        for dim in response_data.get('structure', {}).get('dimensions', {}).get('observation', []):
            if dim.get('id') == 'TIME_PERIOD' and dim.get('values'):
                periods = [period.get('id') for period in dim['values']]
                break

        if not periods:
            error_msg = "No time periods found in ECB response"
            print(error_msg)
            workflow_run.status = "FAILED"
            workflow_run.error_message = error_msg
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.save()
            return

        print(f"Found {len(periods)} time periods in ECB data")

        # Get values from the first series in the dataset
        values = {}

        if response_data.get('dataSets') and response_data['dataSets'][0].get('series'):
            # Get the first series
            first_series = next(iter(response_data['dataSets'][0]['series'].values()))

            if first_series.get('observations'):
                for obs_key, obs_data in first_series['observations'].items():
                    period_idx = int(obs_key)
                    if period_idx < len(periods):
                        period = periods[period_idx]
                        value = obs_data[0] if obs_data else None
                        values[period] = value

        if not values:
            error_msg = "No data values found in ECB response"
            print(error_msg)
            workflow_run.status = "FAILED"
            workflow_run.error_message = error_msg
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.save()
            return

        print(f"Found {len(values)} data points in ECB response")

        # Dict to store changes for action log
        indicator_changes = []

        # Update indicator data
        with transaction.atomic():
            for period, value in values.items():
                if value is None:
                    continue

                try:
                    decimal_value = Decimal(str(value))
                except (decimal.InvalidOperation, TypeError):
                    print(f"Could not convert value {value} to Decimal for period {period}")
                    continue

                # Find or create Data
                existing_data = Data.objects.filter(
                    indicator=indicator,
                    period=period
                ).first()

                if existing_data:
                    old_val = existing_data.value
                    if old_val != decimal_value:
                        existing_data.value = decimal_value
                        existing_data.save()

                        # Add change to our collection
                        indicator_changes.append({
                            'period': period,
                            'data_id': existing_data.id,
                            'old_value': str(old_val),
                            'new_value': str(decimal_value)
                        })
                else:
                    new_data = Data.objects.create(
                        indicator=indicator,
                        period=period,
                        value=decimal_value
                    )

                    # Add change to our collection
                    indicator_changes.append({
                        'period': period,
                        'data_id': new_data.id,
                        'old_value': 'None',
                        'new_value': str(decimal_value)
                    })

            # Create an ActionLog if there are changes
            if indicator_changes:
                ActionLog.objects.create(
                    user=None,
                    indicator_id=indicator.id,
                    run=workflow_run,
                    action_type='DATA_UPDATE',
                    details=indicator_changes
                )
                print(f"Created action log with {len(indicator_changes)} changes for indicator {indicator.name}")

                # Update dependent custom indicators
                update_dependent_custom_indicators(indicator, None)

            # Mark workflow as completed
            workflow_run.status = "COMPLETED"
            workflow_run.success = True
            workflow_run.end_time = timezone.now()
            workflow_run.save()

            # Update last run time and calculate next run
            workflow.last_run = timezone.now()
            workflow.last_run_success = True
            try:
                from koe_db.workflow_views import calculate_next_run
                workflow.next_run = calculate_next_run(workflow.schedule_cron)
            except Exception as e:
                print(f"Error calculating next run: {str(e)}")
            workflow.save()

    except ECBRequest.DoesNotExist:
        error_msg = f"ECBRequest with ID {ecb_request_id} does not exist."
        print(error_msg)
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message = error_msg
            workflow_run.save()
    except Exception as e:
        error_msg = f"An error occurred during ECB request execution: {str(e)}"
        print(error_msg)
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.error_message = error_msg
            workflow_run.end_time = timezone.now()
            workflow_run.save()


@shared_task
def execute_eurostat_request(eurostat_request_id):
    """
    Executes a Eurostat request and processes data for mapped indicators.
    """
    workflow_run = None
    try:
        # Get the Eurostat request and associated workflow
        eurostat_request = EuroStatRequest.objects.get(id=eurostat_request_id)
        workflow = eurostat_request.workflow

        # Create a WorkflowRun
        workflow_run = WorkflowRun.objects.create(
            workflow=workflow,
            start_time=timezone.now(),
            status="RUNNING",
            success=False
        )

        print(f"Executing Eurostat request for workflow: {eurostat_request.workflow.name}")

        # Fetch data from Eurostat API
        try:
            response = requests.get(eurostat_request.url)
            response.raise_for_status()
            json_data = response.json()
        except Exception as e:
            error_msg = f"Failed to fetch data from Eurostat API: {str(e)}"
            print(error_msg)
            if workflow_run:
                workflow_run.status = "FAILED"
                workflow_run.error_message = error_msg
                workflow_run.success = False
                workflow_run.end_time = timezone.now()
                workflow_run.save()
            return

        # Get indicator mappings for this request
        indicator_mappings = EuroStatIndicatorMapping.objects.filter(eurostat_request=eurostat_request)
        if not indicator_mappings:
            error_msg = f"No indicator mappings found for Eurostat request {eurostat_request_id}"
            print(error_msg)
            if workflow_run:
                workflow_run.status = "FAILED"
                workflow_run.error_message = error_msg
                workflow_run.success = False
                workflow_run.end_time = timezone.now()
                workflow_run.save()
            return

        # Extract dimensions information from response
        dimensions = []
        dimension_sizes = {}
        dimension_indices = {}

        if 'dimension' in json_data:
            for dim_key, dim_data in json_data['dimension'].items():
                categories = []

                if 'category' in dim_data and 'index' in dim_data['category']:
                    # Get indices for this dimension
                    categories = list(dim_data['category']['index'].keys())
                    dimension_size = len(categories)
                    dimension_sizes[dim_key] = dimension_size

                    # Store category indices for each dimension
                    dimension_indices[dim_key] = {}
                    for cat_key, cat_index in dim_data['category']['index'].items():
                        dimension_indices[dim_key][cat_key] = cat_index

                dimensions.append({
                    'id': dim_key,
                    'size': dimension_sizes.get(dim_key, 0),
                    'is_time': dim_key == 'time'
                })

        # Calculate size products for all dimensions
        dimension_keys = [d['id'] for d in dimensions]
        size_products = {}
        running_product = 1

        # Calculate products (last dimension changes fastest)
        for dim_key in reversed(dimension_keys):
            size_products[dim_key] = running_product
            running_product *= dimension_sizes.get(dim_key, 1)

        print(f"Dimension sizes: {dimension_sizes}")
        print(f"Size products: {size_products}")

        # Get time periods
        periods = {}
        if 'time' in dimension_indices:
            for period_key, period_index in dimension_indices['time'].items():
                periods[period_index] = period_key

        # Process data for each indicator mapping
        indicator_data = {}

        for mapping in indicator_mappings:
            try:
                indicator = Indicator.objects.get(id=mapping.indicator_id)
                print(f"Processing data for indicator: {indicator.name} ({indicator.id})")

                # Get dimension values for this indicator mapping
                dimension_values = mapping.dimension_values or {}

                # Calculate the array indices for this mapping
                matching_indices = []

                # Start with all indices if we have value data
                if 'value' in json_data:
                    all_indices = list(map(int, json_data['value'].keys()))

                    # Filter indices that match our dimension values
                    for index in all_indices:
                        matches_all_dimensions = True
                        remaining_index = index

                        # Check each non-time dimension
                        for dim_key in dimension_keys:
                            if dim_key == 'time':
                                continue  # Skip time dimension

                            if dim_key in dimension_values:
                                # Get expected category key for this dimension
                                expected_cat_key = dimension_values[dim_key]

                                # Calculate which category this index points to
                                product = size_products.get(dim_key, 1)
                                dim_size = dimension_sizes.get(dim_key, 1)

                                if dim_size > 0:
                                    dim_index = (remaining_index // product) % dim_size

                                    # Get the category key for this index
                                    actual_cat_key = None
                                    for cat_key, cat_index in dimension_indices[dim_key].items():
                                        if cat_index == dim_index:
                                            actual_cat_key = cat_key
                                            break

                                    if actual_cat_key != expected_cat_key:
                                        matches_all_dimensions = False
                                        break

                                # Subtract the processed portion from remaining index
                                remaining_index -= ((remaining_index // product) % dim_size) * product

                        if matches_all_dimensions:
                            matching_indices.append(index)

                    # Process matching indices to get period/value pairs
                    with transaction.atomic():
                        indicator_values = []
                        indicator_changes = {}


                        for index in matching_indices:
                            value = json_data['value'].get(str(index))
                            if value is None:
                                continue

                            # Calculate time period for this index
                            remaining_index = index
                            time_product = size_products.get('time', 1)
                            time_size = dimension_sizes.get('time', 1)

                            if time_size > 0 and time_product > 0:
                                time_index = (remaining_index // time_product) % time_size
                                period = periods.get(time_index)

                                if period:
                                    indicator_values.append((period, value))

                        # Store values for this indicator
                        indicator_data[indicator.id] = {
                            'name': indicator.name,
                            'code': indicator.code,
                            'values': indicator_values
                        }






                        # Create or update Data objects for this indicator
                        for period, value in indicator_values:
                            try:
                                decimal_value = Decimal(str(value))
                            except (decimal.InvalidOperation, TypeError):
                                print(f"Invalid value {value} for period {period} in indicator {indicator.name}")
                                continue

                            # Find or create Data
                            existing_data = Data.objects.filter(
                                indicator=indicator,
                                period=period
                            ).first()

                            if existing_data:
                                old_val = existing_data.value
                                if old_val != decimal_value:
                                    existing_data.value = decimal_value
                                    existing_data.save()
                                    # Log the change
                                    # Add to our collection of changes for this indicator
                                    change = {
                                        'period': period,
                                        'data_id': existing_data.id,
                                        'old_value': str(old_val),
                                        'new_value': str(decimal_value)
                                    }
                                    indicator_changes.setdefault(indicator.id, []).append(change)
                            else:
                                new_data = Data.objects.create(
                                    indicator=indicator,
                                    period=period,
                                    value=decimal_value
                                )
                                change = {
                                    'period': period,
                                    'data_id': new_data.id,
                                    'old_value': 'None',
                                    'new_value': str(decimal_value)
                                }
                                indicator_changes.setdefault(indicator.id, []).append(change)

                        # Create an ActionLog for this indicator

                        if indicator_changes:
                            ActionLog.objects.create(
                                user=None,
                                indicator_id=indicator.id,
                                run=workflow_run,
                                action_type='DATA_UPDATE',
                                details=indicator_changes[indicator.id]  # This must be a list of change objects for indicator_history
                            )
                            # Update dependent custom indicators
                            update_dependent_custom_indicators(indicator, None)
                        # Mark workflow as completed
                        if workflow_run:
                            workflow_run.status = "COMPLETED"
                            workflow_run.success = True
                            workflow_run.end_time = timezone.now()
                            workflow_run.save()

                        # Update workflow last run time
                        workflow.last_run = timezone.now()
                        workflow.last_run_success = True
                        try:
                            from koe_db.workflow_views import calculate_next_run
                            workflow.next_run = calculate_next_run(workflow.schedule_cron)
                        except Exception as e:
                            print(f"Error calculating next run: {str(e)}")
                        workflow.save()

            except Indicator.DoesNotExist:
                print(f"Warning: Indicator with ID {mapping.indicator_id} not found")
            except Exception as e:
                print(f"Error processing mapping for indicator {mapping.indicator_id}: {str(e)}")




    except EuroStatRequest.DoesNotExist:
        error_msg = f"EuroStatRequest with ID {eurostat_request_id} does not exist."
        print(error_msg)
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message = error_msg
            workflow_run.save()
    except Exception as e:
        error_msg = f"An error occurred during Eurostat request execution: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        if workflow_run:
            workflow_run.status = "FAILED"
            workflow_run.success = False
            workflow_run.end_time = timezone.now()
            workflow_run.error_message = error_msg
            workflow_run.save()
