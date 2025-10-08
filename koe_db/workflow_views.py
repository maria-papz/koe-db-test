from django.http import JsonResponse
import json
from django.shortcuts import get_object_or_404
from django.utils import timezone
from .models import Workflow, WorkflowRun, ActionLog, Indicator, Data
from .authentication import CustomJWTAuthentication
from .api_views import get_user
from .permissions import check_indicator_permission

from django_celery_beat.models import PeriodicTask, CrontabSchedule
from koe_db.models import Workflow, CyStatRequest, CyStatIndicatorMapping, Indicator, ECBRequest, WorkflowRun, ActionLog, EuroStatRequest, EuroStatIndicatorMapping
from koe_db.authentication import CustomJWTAuthentication
import requests
from django.db import transaction
from django.utils import timezone
from datetime import datetime, timedelta
import re
from croniter import croniter
from koe_db.tasks import execute_cystat_request, execute_ecb_request, execute_eurostat_request

def get_user(request):
    auth = CustomJWTAuthentication()
    user = auth.authenticate(request)
    if not user:
        return None
    else:
        from koe_db.models import UserAccount
        user_account = UserAccount.objects.get(id=user[0].id)
        return user_account

def calculate_next_run(cron_expression):
    """
    Calculate the next run time from now based on a standard 5-part cron expression.
    """
    now = timezone.now()
    if not croniter.is_valid(cron_expression):
        raise ValueError("Invalid or unsupported cron expression")
    return croniter(cron_expression, now).get_next(datetime)

def schedule_workflow(workflow):
    """
    Create or update the Celery Beat schedule for a workflow.
    """

    if workflow.workflow_type == "CYSTAT":
        cystat_request = CyStatRequest.objects.filter(workflow=workflow).first()
        if not cystat_request:
            print(f"No CyStat request found for workflow: {workflow.name}")
            return

        try:
            # Use the cron expression from the workflow
            cron_parts = workflow.schedule_cron.split()
            crontab, _ = CrontabSchedule.objects.get_or_create(
                minute=cron_parts[0],
                hour=cron_parts[1],
                day_of_month=cron_parts[2],
                month_of_year=cron_parts[3],
                day_of_week=cron_parts[4],
            )

            # Create or update the periodic task
            task_name = f"execute_cystat_request_{workflow.id}"
            PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    "task": "koe_db.tasks.execute_cystat_request",
                    "crontab": crontab,
                    "args": json.dumps([cystat_request.id]),
                    "enabled": workflow.is_active,
                },
            )
            print(f"Scheduled workflow: {workflow.name} with cron: {workflow.schedule_cron}")
        except Exception as e:
            print(f"Failed to schedule workflow {workflow.name}: {e}")

    elif workflow.workflow_type == "ECB":
        ecb_request = ECBRequest.objects.filter(workflow=workflow).first()
        if not ecb_request:
            print(f"No ECB request found for workflow: {workflow.name}")
            return

        try:
            # Use the cron expression from the workflow
            cron_parts = workflow.schedule_cron.split()
            crontab, _ = CrontabSchedule.objects.get_or_create(
                minute=cron_parts[0],
                hour=cron_parts[1],
                day_of_month=cron_parts[2],
                month_of_year=cron_parts[3],
                day_of_week=cron_parts[4],
            )

            # Create or update the periodic task
            task_name = f"execute_ecb_request_{workflow.id}"
            PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    "task": "koe_db.tasks.execute_ecb_request",
                    "crontab": crontab,
                    "args": json.dumps([ecb_request.id]),
                    "enabled": workflow.is_active,
                },
            )
            print(f"Scheduled workflow: {workflow.name} with cron: {workflow.schedule_cron}")
        except Exception as e:
            print(f"Failed to schedule workflow {workflow.name}: {e}")

    elif workflow.workflow_type == "EUROSTAT":
        eurostat_request = EuroStatRequest.objects.filter(workflow=workflow).first()
        if not eurostat_request:
            print(f"No Eurostat request found for workflow: {workflow.name}")
            return

        try:
            # Use the cron expression from the workflow
            cron_parts = workflow.schedule_cron.split()
            crontab, _ = CrontabSchedule.objects.get_or_create(
                minute=cron_parts[0],
                hour=cron_parts[1],
                day_of_month=cron_parts[2],
                month_of_year=cron_parts[3],
                day_of_week=cron_parts[4],
            )

            # Create or update the periodic task
            task_name = f"execute_eurostat_request_{workflow.id}"
            PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    "task": "koe_db.tasks.execute_eurostat_request",
                    "crontab": crontab,
                    "args": json.dumps([eurostat_request.id]),
                    "enabled": workflow.is_active,
                },
            )
            print(f"Scheduled workflow: {workflow.name} with cron: {workflow.schedule_cron}")
        except Exception as e:
            print(f"Failed to schedule workflow {workflow.name}: {e}")

def delete_workflow_schedule(workflow):
    """
    Delete the Celery Beat schedule for a workflow.
    """
    try:
        task_name = f"execute_cystat_request_{workflow.id}" if workflow.workflow_type == "CYSTAT" else f"execute_ecb_request_{workflow.id}" if workflow.workflow_type == "ECB" else f"execute_eurostat_request_{workflow.id}"
        PeriodicTask.objects.filter(name=task_name).delete()
        print(f"Deleted schedule for workflow: {workflow.name}")
    except Exception as e:
        print(f"Failed to delete schedule for workflow {workflow.name}: {e}")

def workflows(request):
    """List all workflows or create a new one"""
    if request.method == 'GET':
        try:
            workflows = Workflow.objects.all()
            workflow_list = []
            for workflow in workflows:
                # Get the latest run for each workflow
                last_run = WorkflowRun.objects.filter(workflow=workflow).order_by('-start_time').first()
                if last_run:
                    workflow.last_run = last_run.start_time
                else:
                    workflow.last_run = None
                workflow_list.append({
                    'id': workflow.id,
                    'name': workflow.name,
                    'workflow_type': workflow.workflow_type,
                    'is_active': workflow.is_active,
                    'schedule_cron': workflow.schedule_cron,
                    'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                    'last_run': workflow.last_run.isoformat() if workflow.last_run else None,
                    'last_run_success': last_run.success if last_run else None,
                })
            return JsonResponse(workflow_list, safe=False)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

    elif request.method == 'POST':
        try:
            data = json.loads(request.body)
            workflow_type = data.get('workflow_type')
            name = data.get('name', 'Untitled Workflow')
            schedule_cron = data.get('schedule_cron', '0 0 1 * *')  # Default to monthly
            is_active = data.get('is_active', True)

            # Calculate next_run based on the cron expression
            try:
                next_run = calculate_next_run(schedule_cron)
            except ValueError as e:
                return JsonResponse({'error': f'Invalid cron expression: {str(e)}'}, status=400)

            workflow = Workflow.objects.create(
                name=name,
                workflow_type=workflow_type,
                schedule_cron=schedule_cron,
                is_active=is_active,
                next_run=next_run
            )

            schedule_workflow(workflow)

            return JsonResponse({
                'success': 'Workflow created successfully',
                'id': workflow.id,
                'name': workflow.name
            })
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def workflows_by_indicator(request, indicator_id):
    """Get workflows associated with a specific indicator"""
    if request.method == 'GET':
        try:
            # Find workflows that have this indicator mapped

            # Check CyStat workflows
            cystat_mappings = CyStatIndicatorMapping.objects.filter(indicator_id=indicator_id)
            cystat_request_ids = [m.cystat_request_id for m in cystat_mappings]
            cystat_workflows = Workflow.objects.filter(cystat_request__in=cystat_request_ids)

            # Check ECB workflows
            ecb_workflows = Workflow.objects.filter(ecb_request__indicator_id=indicator_id)

            # Check Eurostat workflows
            eurostat_mappings = EuroStatIndicatorMapping.objects.filter(indicator_id=indicator_id)
            eurostat_request_ids = [m.eurostat_request_id for m in eurostat_mappings]
            eurostat_workflows = Workflow.objects.filter(eurostat_request__in=eurostat_request_ids)

            # Combine all workflows
            combined_workflows = list(cystat_workflows) + list(ecb_workflows) + list(eurostat_workflows)

            # Remove duplicates
            unique_workflows = {}
            for workflow in combined_workflows:
                if workflow.id not in unique_workflows:
                    # Get the latest run for each workflow
                    last_run = WorkflowRun.objects.filter(workflow=workflow).order_by('-start_time').first()

                    # Get the source-specific data and user-friendly URL
                    source_data = {}
                    user_url = None

                    if workflow.workflow_type == "CYSTAT":
                        try:
                            cystat_request = CyStatRequest.objects.get(workflow=workflow)
                            source_data = {
                                "url": cystat_request.url
                            }

                            # Convert API URL to user-friendly URL
                            api_url = cystat_request.url
                            # Sample API URL: https://cystatdb.cystat.gov.cy:443/api/v1/en/DB1/CPI/
                            if api_url and "api/v1" in api_url:
                                try:
                                    # Extract parts from the URL
                                    parts = api_url.split("/api/v1/")
                                    base = parts[0]
                                    path_parts = parts[1].strip("/").split("/")

                                    if len(path_parts) >= 2:
                                        lang = path_parts[0]
                                        db_id = path_parts[1]
                                        folder_path = "/".join(path_parts[2:-1]) if len(path_parts) > 3 else ""
                                        pxfile = path_parts[-1]

                                        # Convert folder path
                                        folder_path_converted = folder_path.replace("/", "__")

                                        # Construct user-friendly URL
                                        user_url = f"{base}/pxweb/{lang}/{db_id}/{db_id}__{folder_path_converted}/{pxfile}/table/tableViewLayout1/?showtablequery=true"
                                except Exception as e:
                                    print(f"Error converting CyStat URL: {e}")
                        except CyStatRequest.DoesNotExist:
                            pass

                    elif workflow.workflow_type == "ECB":
                        try:
                            ecb_request = ECBRequest.objects.get(workflow=workflow)
                            source_data = {
                                "table": ecb_request.table,
                                "parameters": ecb_request.parameters,
                            }

                            # Convert to user-friendly URL
                            user_url = f"https://data.ecb.europa.eu/data/datasets/{ecb_request.table}/{ecb_request.table}.{ecb_request.parameters}"
                        except ECBRequest.DoesNotExist:
                            pass

                    elif workflow.workflow_type == "EUROSTAT":
                        try:
                            eurostat_request = EuroStatRequest.objects.get(workflow=workflow)
                            source_data = {
                                "url": eurostat_request.url,
                            }

                            # Convert API URL to user-friendly URL
                            api_url = eurostat_request.url
                            # Sample API URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/tipslm14/1.0/
                            if api_url and "eurostat/api" in api_url and "dataflow" in api_url:
                                try:
                                    # Extract dataset ID
                                    parts = api_url.split("/dataflow/")
                                    if len(parts) > 1:
                                        dataset_parts = parts[1].split("/")
                                        if len(dataset_parts) > 1:
                                            dataset_id = dataset_parts[1]
                                            user_url = f"https://ec.europa.eu/eurostat/databrowser/view/{dataset_id}/default/table?lang=en"
                                except Exception as e:
                                    print(f"Error converting Eurostat URL: {e}")
                        except EuroStatRequest.DoesNotExist:
                            pass

                    # Build the workflow data object
                    workflow_data = {
                        'id': workflow.id,
                        'name': workflow.name,
                        'workflow_type': workflow.workflow_type,
                        'is_active': workflow.is_active,
                        'schedule_cron': workflow.schedule_cron,
                        'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                        'last_run': last_run.start_time.isoformat() if last_run else None,
                        'last_run_success': last_run.success if last_run else None,
                        'source_data': source_data,
                        'user_url': user_url
                    }
                    unique_workflows[workflow.id] = workflow_data

            return JsonResponse(list(unique_workflows.values()), safe=False)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)

def workflow_detail(request, id):
    """Retrieve, update or delete a specific workflow"""
    try:
        workflow = Workflow.objects.get(id=id)
    except Workflow.DoesNotExist:
        return JsonResponse({'error': 'Workflow not found'}, status=404)

    # get latest workflow run
    try:
        last_run = WorkflowRun.objects.filter(workflow=workflow).order_by('-start_time').first()
    except WorkflowRun.DoesNotExist:
        last_run = None

    if request.method == 'GET':
        # Return workflow details
        if workflow.workflow_type == "CYSTAT":
            return cystat_workflow_details(id)
        elif workflow.workflow_type == "ECB":
            return ecb_workflow_details(id)
        elif workflow.workflow_type == "EUROSTAT":
            return eurostat_workflow_details(id)
        else:
            return JsonResponse({
                'id': workflow.id,
                'name': workflow.name,
                'workflow_type': workflow.workflow_type,
                'is_active': workflow.is_active,
                'schedule_cron': workflow.schedule_cron,
                'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                'last_run': last_run.start_time.isoformat() if last_run else None,
                'last_run_success': last_run.success if last_run else None,
            })

    elif request.method in ['PUT', 'PATCH']:
        try:
            data = json.loads(request.body)

            # Update only the fields that were provided
            if 'name' in data:
                workflow.name = data['name']

            if 'workflow_type' in data:
                workflow.workflow_type = data['workflow_type']

            if 'is_active' in data:
                workflow.is_active = data['is_active']

            if 'schedule_cron' in data:
                workflow.schedule_cron = data['schedule_cron']

                # Update next_run when schedule changes
                try:
                    workflow.next_run = calculate_next_run(workflow.schedule_cron)
                except ValueError as e:
                    return JsonResponse({'error': f'Invalid cron expression: {str(e)}'}, status=400)

            # Save the updated workflow
            workflow.save()

            # Reschedule the workflow
            schedule_workflow(workflow)

            # Return the updated workflow data
            return JsonResponse({
                'id': workflow.id,
                'name': workflow.name,
                'workflow_type': workflow.workflow_type,
                'is_active': workflow.is_active,
                'schedule_cron': workflow.schedule_cron,
                'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
            })

        except Exception as e:
            return JsonResponse({'error': f'Failed to update workflow: {str(e)}'}, status=500)

    elif request.method == 'DELETE':
        try:
            # First delete the Celery Beat schedule
            delete_workflow_schedule(workflow)

            # Delete any associated requests
            if workflow.workflow_type == "CYSTAT":
                from koe_db.models import CyStatRequest
                CyStatRequest.objects.filter(workflow=workflow).delete()

                # Delete any associated indicator mappings
                from koe_db.models import CyStatIndicatorMapping
                CyStatIndicatorMapping.objects.filter(cystat_request__workflow=workflow).delete()

            # Finally delete the workflow itself
            workflow_name = workflow.name  # Store name before deletion
            workflow.delete()

            return JsonResponse({
                'success': True,
                'message': f'Workflow "{workflow_name}" deleted successfully'
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Failed to delete workflow: {str(e)}'
            }, status=500)

def workflow_run(request, id):
    """Manually run a workflow"""
    if request.method == 'POST':
        try:
            workflow = Workflow.objects.get(id=id)

            if workflow.workflow_type == "CYSTAT":
                # Fetch the associated CyStatRequest
                cystat_request = CyStatRequest.objects.filter(workflow=workflow).first()
                if not cystat_request:
                    return JsonResponse({'error': 'No CyStat request associated with this workflow.'}, status=400)

                # Trigger the Celery task for CyStat workflows
                execute_cystat_request.delay(cystat_request.id)
                return JsonResponse({
                    'success': 'CyStat workflow execution queued',
                    'workflow_id': workflow.id
                })
            elif workflow.workflow_type == "ECB":
                # Fetch the associated ECBRequest
                ecb_request = ECBRequest.objects.filter(workflow=workflow).first()
                if not ecb_request:
                    return JsonResponse({'error': 'No ECB request associated with this workflow.'}, status=400)

                # Trigger the Celery task for ECB workflows
                execute_ecb_request.delay(ecb_request.id)
                return JsonResponse({
                    'success': 'ECB workflow execution queued',
                    'workflow_id': workflow.id
                })
            elif workflow.workflow_type == "EUROSTAT":
                # Fetch the associated EuroStatRequest
                eurostat_request = EuroStatRequest.objects.filter(workflow=workflow).first()
                if not eurostat_request:
                    return JsonResponse({'error': 'No Eurostat request associated with this workflow.'}, status=400)

                # Trigger the Celery task for Eurostat workflows
                execute_eurostat_request.delay(eurostat_request.id)
                return JsonResponse({
                    'success': 'Eurostat workflow execution queued',
                    'workflow_id': workflow.id
                })

            # Handle other workflow types if needed
            return JsonResponse({
                'error': f"Workflow type '{workflow.workflow_type}' is not supported for manual execution."
            }, status=400)

        except Workflow.DoesNotExist:
            return JsonResponse({'error': 'Workflow not found'}, status=404)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def workflow_toggle(request, id):
    """Toggle the active state of a workflow"""
    if request.method == 'POST':
        try:
            with transaction.atomic():
                workflow = Workflow.objects.get(id=id)
                data = json.loads(request.body)

                workflow.is_active = data.get('is_active', not workflow.is_active)
                workflow.save()

                task_name = f"execute_cystat_request_{workflow.id}"
                periodic_task = PeriodicTask.objects.filter(name=task_name).first()
                if periodic_task:
                    periodic_task.enabled = workflow.is_active
                    periodic_task.save()
                else:
                    print(f"PeriodicTask with name '{task_name}' not found.")

            return JsonResponse({
                'success': f"Workflow {'activated' if workflow.is_active else 'deactivated'} successfully",
                'is_active': workflow.is_active
            })

        except Workflow.DoesNotExist:
            return JsonResponse({'error': 'Workflow not found'}, status=404)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def workflow_history(request, id):
    """Get workflow execution history"""
    if request.method == 'GET':
        try:
            workflow = Workflow.objects.get(id=id)
            # Assuming there's a WorkflowRun model to track execution history
            from koe_db.models import WorkflowRun

            runs = WorkflowRun.objects.filter(workflow=workflow).order_by('-start_time')[:10]  # Latest 10 runs

            history = [{
                'id': run.id,
                'start_time': run.start_time.isoformat(),
                'end_time': run.end_time.isoformat() if run.end_time else None,
                'status': run.status,
                'success': run.success,
                'error_message': run.error_message,
                'indicators_updated': run.indicators_updated
            } for run in runs]

            return JsonResponse(history, safe=False)

        except Workflow.DoesNotExist:
            return JsonResponse({'error': 'Workflow not found'}, status=404)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def cystat_workflow_config(request):
    """Configure a CyStat workflow"""
    if request.method == 'POST':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)
            workflow_id = data.get('workflow_id')
            url = data.get('url')
            frequency = data.get('frequency')
            start_period = data.get('start_period', '')
            cystat_request_id = data.get('cystat_request_id')  # Check if a request ID is provided

            # Fetch workflow title from URL
            try:
                response = requests.get(url)
                response_data = response.json()
                workflow_title = response_data.get('title', 'CyStat Workflow')
            except Exception as e:
                return JsonResponse({'error': f'Failed to fetch data from URL: {str(e)}'}, status=400)

            with transaction.atomic():
                workflow = Workflow.objects.get(id=workflow_id)
                if not workflow.name:
                    workflow.name = workflow_title
                    workflow.save()

                # If cystat_request_id is provided, try to update the existing record
                if cystat_request_id:
                    try:
                        cystat_request = CyStatRequest.objects.get(id=cystat_request_id)
                        cystat_request.url = url
                        cystat_request.frequency = frequency
                        cystat_request.start_period = start_period
                        cystat_request.save()
                    except CyStatRequest.DoesNotExist:
                        # If the record doesn't exist, create a new one
                        cystat_request = CyStatRequest.objects.create(
                            workflow=workflow,
                            url=url,
                            request_body={},
                            frequency=frequency,
                            start_period=start_period
                        )
                else:
                    # Create a new record if no ID is provided
                    cystat_request = CyStatRequest.objects.create(
                        workflow=workflow,
                        url=url,
                        request_body={},
                        frequency=frequency,
                        start_period=start_period
                    )

            schedule_workflow(workflow)

            return JsonResponse({
                'success': 'CyStat workflow configured successfully',
                'workflow_id': workflow.id,
                'cystat_request_id': cystat_request.id,
                'title': workflow_title
            })

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def cystat_workflow_details(id):
    """Get detailed information about a CyStat workflow including indicator mappings"""

    try:
        workflow = Workflow.objects.get(id=id)
        if workflow.workflow_type != "CYSTAT":
            return JsonResponse({'error': 'Not a CyStat workflow'}, status=400)

        # Get the CyStat request
        try:
            cystat_request = CyStatRequest.objects.get(workflow=workflow)

            # Get the structure data from URL
            structure_data = None
            try:
                response = requests.get(cystat_request.url)
                json_data = response.json()

                # Extract variables from the API
                variables = json_data.get('variables', [])
                title = json_data.get('title', '')

                # Find which variable is time-based
                time_index = None
                time_code = None
                for index, variable in enumerate(variables):
                    code = variable.get('code')
                    if code in ['QUARTER', 'MONTH','YEAR']:
                        time_index = index
                        time_code = code
                        break

                # Format time periods
                periods = []
                if time_index is not None and time_code:
                    time_variable = variables[time_index]
                    raw_periods = time_variable.get('valueTexts', [])

                    if time_code == 'QUARTER':
                        periods = [period[:4] + '-' + period[4:] for period in raw_periods]
                    else:  # MONTH
                        periods = [period[:4] + '-' + period[4:].zfill(2) for period in raw_periods]

                # Enhance structure data to include actual values along with valueTexts
                for var in variables:
                    # If the variable has values property, make sure it's included
                    if 'values' not in var and 'valueTexts' in var:
                        var['values'] = var.get('values', [])
                        # If values is empty but valueTexts exists, create placeholder values
                        if not var['values'] and var['valueTexts']:
                            # Default to indices if actual values not provided
                            var['values'] = list(range(len(var['valueTexts'])))

                structure_data = {
                    'title': title,
                    'variables': variables,
                    'time_index': time_index,
                    'time_code': time_code,
                    'periods': periods
                }
            except Exception as e:
                print(f"Error fetching structure data: {str(e)}")
                structure_data = None

            # Get indicator mappings - improved with detailed logging
            mappings = []
            indicators = []

            # Fetch all mappings for this request
            indicator_mappings = CyStatIndicatorMapping.objects.filter(cystat_request=cystat_request)
            print(f"Found {indicator_mappings.count()} indicator mappings for request {cystat_request.id}")

            # Process each mapping
            for mapping in indicator_mappings:
                try:
                    indicator = Indicator.objects.get(id=mapping.indicator_id)
                    print(f"Found indicator: {indicator.name} ({indicator.id})")

                    indicators.append({
                        'id': indicator.id,
                        'name': indicator.name,
                        'code': indicator.code
                    })

                    # Make sure key_indices is not None
                    key_indices = mapping.key_indices or {}
                    print(f"Mapping key indices: {key_indices}")

                    mappings.append({
                        'indicator_id': mapping.indicator_id,
                        'indicator_code': indicator.code,
                        'indicator_name': indicator.name,
                        'mapping_id': mapping.id,
                        'key_indices': key_indices
                    })
                except Indicator.DoesNotExist:
                    print(f"Warning: Indicator with ID {mapping.indicator_id} not found")

            # Add more mappings debug info
            print(f"Total mappings prepared: {len(mappings)}")
            print(f"Total indicators prepared: {len(indicators)}")

            result = {
                'id': workflow.id,
                'name': workflow.name,
                'workflow_type': workflow.workflow_type,
                'is_active': workflow.is_active,
                'schedule_cron': workflow.schedule_cron,
                'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                'last_run': workflow.last_run.isoformat() if workflow.last_run else None,
                'cystat_request_id': cystat_request.id,
                'url': cystat_request.url,
                'frequency': cystat_request.frequency,
                'start_period': cystat_request.start_period,
                'request_body': cystat_request.request_body,
                'data_structure': structure_data,
                'indicators': indicators,
                'indicator_mappings': mappings
            }

            return JsonResponse(result)

        except CyStatRequest.DoesNotExist:
            return JsonResponse({
                'error': 'CyStat request not found for this workflow',
                'workflow': {
                    'id': workflow.id,
                    'name': workflow.name,
                    'workflow_type': workflow.workflow_type,
                    'is_active': workflow.is_active,
                    'schedule_cron': workflow.schedule_cron,
                }
            }, status=404)

    except Workflow.DoesNotExist:
        return JsonResponse({'error': 'Workflow not found'}, status=404)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)

def cystat_indicator_mapping(request):
    """Map indicators to CyStat data fields"""
    if request.method == 'POST':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)
            cystat_request_id = data.get('cystat_request_id')
            indicator_mappings = data.get('indicator_mappings', [])
            is_update = data.get('is_update', False)  # Check if this is an update

            cystat_request = CyStatRequest.objects.get(id=cystat_request_id)

            # Fetch variable data to get actual values
            structure_data = None
            try:
                response = requests.get(cystat_request.url)
                json_data = response.json()
                variables = json_data.get('variables', [])
                structure_data = {
                    'variables': variables
                }
            except Exception as e:
                print(f"Error fetching structure data for mapping: {str(e)}")
                return JsonResponse({'error': f'Error fetching data structure: {str(e)}'}, status=500)

            # Construct the request body based on mappings
            query = {
                "query": [],
                "response": {
                    "format": "json"
                }
            }

            # Group mappings by code for efficient query building
            code_mappings = {}
            for mapping in indicator_mappings:
                indicator_id = mapping.get('indicator_id')
                for code_item in mapping.get('code_mappings', []):
                    code = code_item.get('code')
                    value_index = code_item.get('value')  # This is the index in the valueTexts array

                    # Find the actual value for this code and index
                    for variable in structure_data['variables']:
                        if variable.get('code') == code:
                            # If we have 'values' use that
                            if 'values' in variable and len(variable['values']) > int(value_index):
                                actual_value = str(variable['values'][int(value_index)])
                            # Otherwise fallback to the valueTexts index
                            else:
                                actual_value = value_index
                            break

                    # If we couldn't find an actual value, use the index as fallback
                    if actual_value is None:
                        actual_value = value_index

                    if code not in code_mappings:
                        code_mappings[code] = {"code": code, "values": []}

                    if actual_value not in code_mappings[code]["values"]:
                        code_mappings[code]["values"].append(actual_value)

            # Build the query
            for code, mapping in code_mappings.items():
                if code.lower() != "quarter" and code.lower() != "month":  # Skip time-based fields
                    query['query'].append({
                        "code": code,
                        "selection": {
                            "filter": "item",
                            "values": mapping["values"]
                        }
                    })

            with transaction.atomic():
                # Update the request body
                cystat_request.request_body = query
                cystat_request.save()

                # If updating, delete old mappings first
                if is_update:
                    CyStatIndicatorMapping.objects.filter(cystat_request=cystat_request).delete()

                # Create indicator mappings
                for mapping in indicator_mappings:
                    indicator_id = mapping.get('indicator_id')
                    key_indices = {}

                    for code_mapping in mapping.get('code_mappings', []):
                        code = code_mapping.get('code')
                        value_index = code_mapping.get('value')

                        # Find the actual value for this code and index
                        for variable in structure_data['variables']:
                            if variable.get('code') == code:
                                # Store the actual value or the index as fallback
                                if 'values' in variable and len(variable['values']) > int(value_index):
                                    key_indices[code] = str(variable['values'][int(value_index)])
                                else:
                                    key_indices[code] = value_index
                                break

                        # If we couldn't find in the variables, use the index as fallback
                        if code not in key_indices:
                            key_indices[code] = value_index

                    CyStatIndicatorMapping.objects.create(
                        cystat_request=cystat_request,
                        indicator_id=indicator_id,
                        key_indices=key_indices
                    )

            return JsonResponse({
                'success': f'Indicator mappings {("updated" if is_update else "created")} successfully',
                'workflow_id': cystat_request.workflow.id
            })

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def fetch_cystat_structure(request):
    """Fetch the structure of a CyStat data source"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            url = data.get('url')

            if not url:
                return JsonResponse({'error': 'URL is required'}, status=400)

            response = requests.get(url)
            json_data = response.json()

            # Extract the structure information
            variables = json_data.get('variables', [])
            title = json_data.get('title', '')

            # Find which variable is time-based (QUARTER/MONTH)
            time_index = None
            time_code = None
            for index, variable in enumerate(variables):
                code = variable.get('code')
                if code in ['QUARTER', 'MONTH']:
                    time_index = index
                    time_code = code
                    break

            # Format time periods if found
            periods = []
            if time_index is not None and time_code:
                time_variable = variables[time_index]
                raw_periods = time_variable.get('valueTexts', [])

                if time_code == 'QUARTER':
                    # Format like "2023-Q1"
                    periods = [period[:4] + '-' + period[4:] for period in raw_periods]
                else:  # MONTH
                    # Format like "2023-01"
                    periods = [period[:4] + '-' + period[4:].zfill(2) for period in raw_periods]

            # Prepare the structure information for the frontend
            structure = {
                'title': title,
                'variables': variables,
                'time_index': time_index,
                'time_code': time_code,
                'periods': periods
            }

            return JsonResponse(structure)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def ecb_workflow_config(request):
    """Configure an ECB workflow"""
    if request.method == 'POST':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)
            workflow_id = data.get('workflow_id')
            table = data.get('table')
            parameters = data.get('parameters')
            frequency = data.get('frequency')
            indicator_id = data.get('indicator_id')
            ecb_request_id = data.get('ecb_request_id')  # Check if a request ID is provided

            # Construct the full URL
            url = f"https://data-api.ecb.europa.eu/service/data/{table}/{parameters}?format=jsondata"

            # Try to fetch the title from the ECB API response
            workflow_title = ''
            try:
                response = requests.get(url)
                response_data = response.json()

                # Find the title in the response data
                title_attrs = None
                for attr in response_data.get('structure', {}).get('attributes', {}).get('series', []):
                    if attr.get('id') == 'TITLE_COMPL':
                        title_attrs = attr
                        break

                if title_attrs and title_attrs.get('values'):
                    workflow_title = title_attrs['values'][0].get('name', '')

                if not workflow_title and 'TITLE' in response_data.get('structure', {}).get('attributes', {}):
                    title_attr = response_data['structure']['attributes']['series'].get('TITLE', {})
                    if title_attr and title_attr.get('values'):
                        workflow_title = title_attr['values'][0].get('name', '')
            except Exception as e:
                print(f"Failed to fetch title from ECB API: {str(e)}")
                workflow_title = f"ECB {table} Data"

            with transaction.atomic():
                workflow = Workflow.objects.get(id=workflow_id)
                if not workflow.name:
                    workflow.name = workflow_title[:255]  # Ensure it fits in the field
                    workflow.save()

                # If ecb_request_id is provided, try to update the existing record
                if ecb_request_id:
                    try:
                        ecb_request = ECBRequest.objects.get(id=ecb_request_id)
                        ecb_request.table = table
                        ecb_request.parameters = parameters
                        ecb_request.frequency = frequency
                        ecb_request.indicator_id = indicator_id
                        ecb_request.save()
                    except ECBRequest.DoesNotExist:
                        # Create a new request if the ID doesn't exist
                        ecb_request = ECBRequest.objects.create(
                            workflow=workflow,
                            table=table,
                            parameters=parameters,
                            frequency=frequency,
                            indicator_id=indicator_id
                        )
                else:
                    # Create a new ECB request
                    ecb_request = ECBRequest.objects.create(
                        workflow=workflow,
                        table=table,
                        parameters=parameters,
                        frequency=frequency,
                        indicator_id=indicator_id
                    )

            schedule_workflow(workflow)

            return JsonResponse({
                'success': 'ECB workflow configured successfully',
                'workflow_id': workflow.id,
                'ecb_request_id': ecb_request.id,
                'title': workflow_title
            })

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

def fetch_ecb_structure(request):
    """Fetch the structure of an ECB data source"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            table = data.get('table')
            parameters = data.get('parameters')

            if not table or not parameters:
                return JsonResponse({'error': 'Table and parameters are required'}, status=400)

            url = f"https://data-api.ecb.europa.eu/service/data/{table}/{parameters}?format=jsondata"
            response = requests.get(url)
            json_data = response.json()

            # Extract title
            title = ""
            title_compl_attr = None
            for attr in json_data.get('structure', {}).get('attributes', {}).get('series', []):
                if attr.get('id') == 'TITLE_COMPL':
                    title_compl_attr = attr
                    break

            if title_compl_attr and title_compl_attr.get('values'):
                title = title_compl_attr['values'][0].get('name', '')

            # Extract frequency information
            frequency = "Monthly"
            time_format_attr = None
            for attr in json_data.get('structure', {}).get('attributes', {}).get('series', []):
                if attr.get('id') == 'TIME_FORMAT':
                    time_format_attr = attr
                    break

            if time_format_attr and time_format_attr.get('values'):
                time_format = time_format_attr['values'][0].get('name', '')

                # Convert ISO 8601 format to readable frequency
                if time_format == 'P1Y':
                    frequency = 'Yearly'
                elif time_format == 'P3M':
                    frequency = 'Quarterly'
                elif time_format == 'P1M':
                    frequency = 'Monthly'
                elif time_format == 'P1D':
                    frequency = 'Daily'

            # Extract time periods
            periods = []
            time_dimension = None
            for dim in json_data.get('structure', {}).get('dimensions', {}).get('observation', []):
                if dim.get('id') == 'TIME_PERIOD':
                    time_dimension = dim
                    break

            if time_dimension and time_dimension.get('values'):
                periods = [period.get('id') for period in time_dimension['values']]

            # Extract data series (first series to get a sample of the data)
            data_sample = None
            if json_data.get('dataSets') and json_data['dataSets'][0].get('series'):
                for series_key, series_data in json_data['dataSets'][0]['series'].items():
                    if series_data.get('observations'):
                        data_sample = []
                        for obs_key, obs_data in series_data['observations'].items():
                            if len(periods) > int(obs_key):
                                period = periods[int(obs_key)]
                                value = obs_data[0] if obs_data else None
                                data_sample.append({"period": period, "value": value})
                        break

            # Build structure information
            structure = {
                'title': title,
                'frequency': frequency,
                'periods': periods,
                'data_sample': data_sample[:10] if data_sample else []  # Just send a few samples
            }

            return JsonResponse(structure)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)

def ecb_workflow_details(id):
    """Get detailed information about an ECB workflow"""
    try:
        workflow = Workflow.objects.get(id=id)
        if workflow.workflow_type != "ECB":
            return JsonResponse({'error': 'Not an ECB workflow'}, status=400)

        # Get the ECB request
        try:
            ecb_request = ECBRequest.objects.get(workflow=workflow)

            # Construct the ECB API URL
            url = f"https://data-api.ecb.europa.eu/service/data/{ecb_request.table}/{ecb_request.parameters}?format=jsondata"

            # Fetch data from the ECB API
            structure_data = None
            try:
                response = requests.get(url)
                json_data = response.json()

                # Extract title
                title = ""
                for attr in json_data.get('structure', {}).get('attributes', {}).get('series', []):
                    if attr.get('id') == 'TITLE_COMPL':
                        if attr.get('values'):
                            title = attr['values'][0].get('name', '')
                        break

                # Extract time periods
                periods = []
                for dim in json_data.get('structure', {}).get('dimensions', {}).get('observation', []):
                    if dim.get('id') == 'TIME_PERIOD':
                        if dim.get('values'):
                            periods = [period.get('id') for period in dim['values']]
                        break

                # Extract frequency
                frequency = ecb_request.frequency

                structure_data = {
                    'title': title,
                    'periods': periods,
                    'frequency': frequency,
                    'table': ecb_request.table,
                    'parameters': ecb_request.parameters
                }
            except Exception as e:
                print(f"Error fetching structure data: {str(e)}")
                structure_data = None

            # Get indicator information
            indicator = Indicator.objects.get(id=ecb_request.indicator_id)

            result = {
                'id': workflow.id,
                'name': workflow.name,
                'workflow_type': workflow.workflow_type,
                'is_active': workflow.is_active,
                'schedule_cron': workflow.schedule_cron,
                'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                'last_run': workflow.last_run.isoformat() if workflow.last_run else None,
                'ecb_request_id': ecb_request.id,
                'table': ecb_request.table,
                'parameters': ecb_request.parameters,
                'frequency': ecb_request.frequency,
                'indicator_id': ecb_request.indicator_id,
                'indicator': {
                    'id': indicator.id,
                    'name': indicator.name,
                    'code': indicator.code
                },
                'data_structure': structure_data
            }

            return JsonResponse(result)

        except ECBRequest.DoesNotExist:
            return JsonResponse({
                'error': 'ECB request not found for this workflow',
                'workflow': {
                    'id': workflow.id,
                    'name': workflow.name,
                    'workflow_type': workflow.workflow_type,
                    'is_active': workflow.is_active,
                    'schedule_cron': workflow.schedule_cron,
                }
            }, status=404)

    except Workflow.DoesNotExist:
        return JsonResponse({'error': 'Workflow not found'}, status=404)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)

def eurostat_workflow_config(request):
    """Configure a Eurostat workflow"""
    if request.method == 'POST':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)
            workflow_id = data.get('workflow_id')
            url = data.get('url')
            frequency = data.get('frequency')
            eurostat_request_id = data.get('eurostat_request_id')  # Check if a request ID is provided

            # Fetch workflow title from URL
            try:
                response = requests.get(url)
                response_data = response.json()
                workflow_title = response_data.get('label', None)
            except Exception as e:
                return JsonResponse({'error': f'Failed to fetch data from URL: {str(e)}'}, status=400)

            with transaction.atomic():
                workflow = Workflow.objects.get(id=workflow_id)
                if not workflow.name:
                    workflow.name = workflow_title
                    workflow.save()

                # If eurostat_request_id is provided, try to update the existing record
                if eurostat_request_id:
                    try:
                        eurostat_request = EuroStatRequest.objects.get(id=eurostat_request_id)
                        eurostat_request.url = url
                        eurostat_request.frequency = frequency
                        eurostat_request.save()
                    except EuroStatRequest.DoesNotExist:
                        # If the record doesn't exist, create a new one
                        eurostat_request = EuroStatRequest.objects.create(
                            workflow=workflow,
                            url=url,
                            frequency=frequency
                        )
                else:
                    # Create a new record if no ID is provided
                    eurostat_request = EuroStatRequest.objects.create(
                        workflow=workflow,
                        url=url,
                        frequency=frequency
                    )

            schedule_workflow(workflow)

            return JsonResponse({
                'success': 'Eurostat workflow configured successfully',
                'workflow_id': workflow.id,
                'eurostat_request_id': eurostat_request.id,
                'title': workflow_title
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)

def eurostat_workflow_details(id):
    """Get detailed information about a Eurostat workflow including indicator mappings"""
    try:
        workflow = Workflow.objects.get(id=id)
        if workflow.workflow_type != "EUROSTAT":
            return JsonResponse({'error': 'Not a Eurostat workflow'}, status=400)

        # Get the Eurostat request
        try:
            eurostat_request = EuroStatRequest.objects.get(workflow=workflow)

            # Get the structure data from URL
            structure_data = None
            try:
                response = requests.get(eurostat_request.url)
                json_data = response.json()

                # Extract title
                title = json_data.get('title', '')

                # Extract dimensions
                dimensions = []
                dimension_sizes = {}

                if 'dimension' in json_data:
                    for dim_key, dim_data in json_data['dimension'].items():
                        if dim_key != 'time' :  # Skip time dimension as it's handled separately
                            dim_label = dim_data.get('label', dim_key)
                            categories = []

                            if 'category' in dim_data:
                                # If the category has a label object, use it to get names
                                if 'label' in dim_data['category']:
                                    for cat_key, cat_label in dim_data['category']['label'].items():
                                        categories.append({
                                            'id': cat_key,
                                            'label': cat_label
                                        })
                                # Otherwise use index keys
                                elif 'index' in dim_data['category']:
                                    for cat_key, cat_index in dim_data['category']['index'].items():
                                        categories.append({
                                            'id': cat_key,
                                            'label': cat_key,
                                            'index': cat_index
                                        })

                            dimensions.append({
                                'id': dim_key,
                                'label': dim_label,
                                'categories': categories
                            })

                            # Store dimension size for index calculation
                            dimension_sizes[dim_key] = len(categories)

                # Extract time periods
                periods = []
                if 'dimension' in json_data and 'time' in json_data['dimension']:
                    time_dim = json_data['dimension']['time']
                    if 'category' in time_dim and 'index' in time_dim['category']:
                        for period_key, period_index in time_dim['category']['index'].items():
                            period_label = time_dim['category'].get('label', {}).get(period_key, period_key)
                            periods.append({
                                'id': period_key,
                                'label': period_label,
                                'index': period_index
                            })
                        # Sort periods by index
                        periods = sorted(periods, key=lambda p: p['index'])

                structure_data = {
                    'title': title,
                    'dimensions': dimensions,
                    'periods': [p['id'] for p in periods],
                    'period_objects': periods,
                    'dimension_sizes': dimension_sizes
                }
            except Exception as e:
                print(f"Error fetching structure data: {str(e)}")
                structure_data = None

            # Get indicator mappings
            mappings = []
            indicators = []

            # Fetch all mappings for this request
            indicator_mappings = EuroStatIndicatorMapping.objects.filter(eurostat_request=eurostat_request)
            print(f"Found {indicator_mappings.count()} indicator mappings for request {eurostat_request.id}")

            # Process each mapping
            for mapping in indicator_mappings:
                try:
                    indicator = Indicator.objects.get(id=mapping.indicator_id)
                    print(f"Found indicator: {indicator.name} ({indicator.id})")

                    indicators.append({
                        'id': indicator.id,
                        'name': indicator.name,
                        'code': indicator.code
                    })

                    # Make sure dimension_values is not None
                    dimension_values = mapping.dimension_values or {}
                    print(f"Mapping dimension values: {dimension_values}")

                    mappings.append({
                        'indicator_id': mapping.indicator_id,
                        'indicator_code': indicator.code,
                        'indicator_name': indicator.name,
                        'mapping_id': mapping.id,
                        'dimension_values': dimension_values
                    })
                except Indicator.DoesNotExist:
                    print(f"Warning: Indicator with ID {mapping.indicator_id} not found")

            result = {
                'id': workflow.id,
                'name': workflow.name,
                'workflow_type': workflow.workflow_type,
                'is_active': workflow.is_active,
                'schedule_cron': workflow.schedule_cron,
                'next_run': workflow.next_run.isoformat() if workflow.next_run else None,
                'last_run': workflow.last_run.isoformat() if workflow.last_run else None,
                'eurostat_request_id': eurostat_request.id,
                'url': eurostat_request.url,
                'frequency': eurostat_request.frequency,
                'data_structure': structure_data,
                'indicators': indicators,
                'indicator_mappings': mappings
            }

            return JsonResponse(result)

        except EuroStatRequest.DoesNotExist:
            return JsonResponse({
                'error': 'Eurostat request not found for this workflow',
                'workflow': {
                    'id': workflow.id,
                    'name': workflow.name,
                    'workflow_type': workflow.workflow_type,
                    'is_active': workflow.is_active,
                    'schedule_cron': workflow.schedule_cron,
                }
            }, status=404)

    except Workflow.DoesNotExist:
        return JsonResponse({'error': 'Workflow not found'}, status=404)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)

def eurostat_indicator_mapping(request):
    """Map indicators to Eurostat data fields"""
    if request.method == 'POST':
        try:
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            data = json.loads(request.body)
            eurostat_request_id = data.get('eurostat_request_id')
            indicator_mappings = data.get('indicator_mappings', [])
            is_update = data.get('is_update', False)  # Check if this is an update

            eurostat_request = EuroStatRequest.objects.get(id=eurostat_request_id)

            with transaction.atomic():
                # If updating, delete old mappings first
                if is_update:
                    EuroStatIndicatorMapping.objects.filter(eurostat_request=eurostat_request).delete()

                # Create indicator mappings
                for mapping in indicator_mappings:
                    indicator_id = mapping.get('indicator_id')
                    dimension_values = {}

                    for dim_mapping in mapping.get('dimension_mappings', []):
                        dimension_id = dim_mapping.get('dimension_id')
                        value = dim_mapping.get('value')
                        dimension_values[dimension_id] = value

                    EuroStatIndicatorMapping.objects.create(
                        eurostat_request=eurostat_request,
                        indicator_id=indicator_id,
                        dimension_values=dimension_values
                    )

            return JsonResponse({
                'success': f'Indicator mappings {("updated" if is_update else "created")} successfully',
                'workflow_id': eurostat_request.workflow.id
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)

def fetch_eurostat_structure(request):
    """Fetch the structure of a Eurostat data source"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            url = data.get('url')

            if not url:
                return JsonResponse({'error': 'URL is required'}, status=400)

            response = requests.get(url)
            json_data = response.json()

            # Extract the title
            title = json_data.get('title', '')

            # Extract frequency from dimension 'freq' if available
            frequency = None
            if ('dimension' in json_data and
                'freq' in json_data['dimension'] and
                'category' in json_data['dimension']['freq'] and
                'label' in json_data['dimension']['freq']['category']):

                freq_labels = json_data['dimension']['freq']['category']['label']
                # Take the first frequency label (usually there's only one)
                if freq_labels:
                    frequency = next(iter(freq_labels.values()))
                    # Convert 'annual' to 'Yearly'
                    if frequency and frequency.lower() == 'annual':
                        frequency = 'Yearly'

            # Extract all dimensions including time
            dimensions = []
            dimension_sizes = {}
            dimension_indices = {}
            dimension_labels = {}

            if 'dimension' in json_data:
                for dim_key, dim_data in json_data['dimension'].items():
                    dim_label = dim_data.get('label', dim_key)
                    dimension_labels[dim_key] = dim_label
                    categories = []

                    if 'category' in dim_data:
                        cat_indices = {}
                        cat_labels = {}

                        # Get all category indices
                        if 'index' in dim_data['category']:
                            for cat_key, cat_index in dim_data['category']['index'].items():
                                cat_indices[cat_key] = cat_index

                        # Get all category labels
                        if 'label' in dim_data['category']:
                            for cat_key, cat_label in dim_data['category']['label'].items():
                                cat_labels[cat_key] = cat_label

                        # Combine them
                        for cat_key, cat_index in cat_indices.items():
                            label = cat_labels.get(cat_key, cat_key)
                            categories.append({
                                'id': cat_key,
                                'label': label,
                                'index': cat_index
                            })
                            # Store the index for each category
                            dimension_indices.setdefault(dim_key, {})[cat_index] = {
                                'id': cat_key,
                                'label': label
                            }

                    # Add to dimensions array (including time)
                    dimensions.append({
                        'id': dim_key,
                        'label': dim_label,
                        'categories': sorted(categories, key=lambda x: x['index']),
                        'is_time': dim_key == 'time'
                    })

                    # Store dimension size for index calculation
                    dimension_sizes[dim_key] = len(categories)

            # Extract time periods (as a convenient separate list)
            periods = []
            if 'dimension' in json_data and 'time' in json_data['dimension']:
                time_dim = json_data['dimension']['time']
                if 'category' in time_dim and 'index' in time_dim['category']:
                    time_labels = time_dim['category'].get('label', {})
                    for period_key, period_index in time_dim['category']['index'].items():
                        period_label = time_labels.get(period_key, period_key)
                        periods.append({
                            'id': period_key,
                            'label': period_label,
                            'index': period_index
                        })
                    # Sort periods by index
                    periods = sorted(periods, key=lambda p: p['index'])

            # Calculate size products for all dimensions
            # The size product represents how many positions in the flat array each unit of a dimension takes
            dimension_keys = [d['id'] for d in dimensions]
            size_products = {}
            running_product = 1

            # Calculate products (last dimension changes fastest)
            for dim_key in reversed(dimension_keys):
                size_products[dim_key] = running_product
                running_product *= dimension_sizes.get(dim_key, 1)

            # Extract data sample with improved information
            data_sample = []

            if 'value' in json_data:
                # Get a few sample values for demonstration
                sample_indices = list(json_data['value'].keys())[:5]  # Take first 5 indices

                # Process each sample index
                for index in sample_indices:
                    try:
                        index_int = int(index)
                        value = json_data['value'][index]

                        # Calculate which dimension values this index corresponds to
                        dimension_values = {}
                        remaining_index = index_int

                        # For each dimension (including time), calculate which category this index refers to
                        for dim_key in dimension_keys:
                            dim_size = dimension_sizes.get(dim_key, 1)
                            if dim_size > 0:
                                product = size_products.get(dim_key, 1)
                                dim_index = (remaining_index // product) % dim_size

                                # Get the dimension value info for this index
                                if dim_key in dimension_indices and dim_index in dimension_indices[dim_key]:
                                    dimension_values[dim_key] = dimension_indices[dim_key][dim_index]

                                # Subtract the processed portion from remaining index
                                remaining_index -= (dim_index * product)

                        # Create the sample entry with complete information
                        sample_entry = {
                            'index': index,
                            'value': value,
                            'dimensions': dimension_values
                        }

                        data_sample.append(sample_entry)
                    except Exception as e:
                        print(f"Error processing index {index}: {e}")
                        continue

            # Prepare the structure information for the frontend
            structure = {
                'title': title,
                'frequency': frequency,
                'dimensions': dimensions,
                'periods': [p['id'] for p in sorted([d for d in periods], key=lambda x: x['index'])],
                'period_labels': {p['id']: p['label'] for p in periods},
                'period_objects': periods,
                'data_sample': data_sample,
                'dimension_sizes': dimension_sizes,
                'size_products': size_products,
                'dimension_labels': dimension_labels
            }

            return JsonResponse(structure)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)

def workflow_run_history(request, workflow_id):
    """Get detailed workflow execution history with action logs"""
    if request.method == 'GET':
        try:
            workflow = Workflow.objects.get(id=workflow_id)
            runs = WorkflowRun.objects.filter(workflow=workflow).order_by('-start_time')[:10]  # Latest 10 runs

            history = []
            for run in runs:
                # Get action logs for this workflow run
                action_logs = ActionLog.objects.filter(run=run).order_by('-timestamp')
                action_logs_data = []

                # Build data for each action log
                for log in action_logs:
                    try:
                        indicator = Indicator.objects.get(id=log.indicator_id)
                        action_logs_data.append({
                            'id': log.id,
                            'indicator_id': log.indicator_id,
                            'indicator_code': indicator.code,
                            'indicator_name': indicator.name,
                            'action_type': log.action_type,
                            'timestamp': log.timestamp.isoformat(),
                            'details': log.details
                        })
                    except Indicator.DoesNotExist:
                        # Handle case where indicator might have been deleted
                        action_logs_data.append({
                            'id': log.id,
                            'indicator_id': log.indicator_id,
                            'indicator_name': 'Unknown Indicator',
                            'action_type': log.action_type,
                            'timestamp': log.timestamp.isoformat(),
                            'details': log.details
                        })

                # Add run data to history
                run_data = {
                    'id': run.id,
                    'start_time': run.start_time.isoformat(),
                    'end_time': run.end_time.isoformat() if run.end_time else None,
                    'status': run.status,
                    'success': run.success,
                    'error_message': run.error_message,
                    'action_logs': action_logs_data
                }
                history.append(run_data)

            return JsonResponse(history, safe=False)

        except Workflow.DoesNotExist:
            return JsonResponse({'error': 'Workflow not found'}, status=404)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

    return JsonResponse({'error': 'Method not allowed'}, status=405)

def latest_workflow_run(request):
    """
    Returns the latest workflow run with indicator data and time series
    """
    try:
        if request.method == 'GET':
            user = get_user(request)
            if not user:
                return JsonResponse({'error': 'User not authenticated'}, status=401)

            # Find workflows with the most recent runs, sorted by last_run date
            workflows_with_runs = Workflow.objects.filter(
                last_run__isnull=False
            ).order_by('-last_run')[:10]  # Get the 10 most recently run workflows

            if not workflows_with_runs.exists():
                return JsonResponse({'message': 'No recent workflow runs found'}, status=404)

            # Check each workflow in order of recency to find one with data updates
            for workflow in workflows_with_runs:
                # Get the most recent run for this workflow
                recent_run = WorkflowRun.objects.filter(
                    workflow=workflow
                ).order_by('-start_time').first()

                if not recent_run:
                    continue

                # Check if this run has associated action logs
                action_logs = ActionLog.objects.filter(
                    run=recent_run
                ).filter(
                    action_type__in=['DATA_UPDATE', 'INDICATOR_EDIT', 'INDICATOR_CREATE']
                )

                if action_logs.exists():
                    # Found a workflow run with data updates
                    # Get the affected indicators
                    affected_indicators = set()
                    for log in action_logs:
                        affected_indicators.add(log.indicator_id)

                    # Build the response data
                    indicators_data = []

                    for indicator_id in affected_indicators:
                        try:
                            indicator = Indicator.objects.get(id=indicator_id)

                            # Check if user has permission to view this indicator
                            if not check_indicator_permission(user, indicator, 'view'):
                                continue

                            # Get indicator data points
                            data_points = Data.objects.filter(indicator=indicator).order_by('period')
                            data_list = []

                            for point in data_points:
                                data_list.append({
                                    'period': point.period,
                                    'value': point.value
                                })

                            indicators_data.append({
                                'id': str(indicator.id),
                                'name': indicator.name,
                                'code': indicator.code,
                                'data': data_list
                            })
                        except Indicator.DoesNotExist:
                            continue

                    if indicators_data:
                        return JsonResponse({
                            'workflow_id': str(workflow.id),
                            'workflow_name': workflow.name,
                            'workflow_description': '',  # Workflow model doesn't have description field
                            'timestamp': recent_run.start_time.isoformat() if recent_run.start_time else recent_run.run_time.isoformat(),
                            'status': recent_run.status,
                            'indicators': indicators_data,
                            'details': {
                                'success': recent_run.success,
                                'error_message': recent_run.error_message
                            }
                        })

            return JsonResponse({'message': 'No workflow runs with indicator updates found'}, status=404)

    except Exception as e:
        print(f"Error in latest_workflow_run: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)
