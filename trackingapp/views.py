import json
import os
from datetime import date, datetime, time, timedelta
from django.utils import timezone

import dateutil.parser as parser
import xlwt
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.db.models import CharField, Count, F, Func, Max, Min, Sum, Value
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.template.loader import get_template
from django.urls import resolve
from django_celery_beat.models import CrontabSchedule, PeriodicTask
from tracking import tasks
from tracking.Crawler.logging_handler import FileLogHandler
from tracking.forms import NotificationForm, WebsiteForm
from tracking.helpers import (calculate_seconds, get_cron_end_time,
                                 get_cron_start_time)
from tracking.models import (LocationDetail, Notification, ScheduledTask,
                                SystemField, TraceReportLog, Website,
                                WebsiteMapping, WebsiteMappingValue)
from xhtml2pdf import pisa
import pytz


@login_required
def run_reports(request):
    if request.method == "POST":
        data = json.load(request)
        pk = data.get("id")
        try:
            scheduled_task = ScheduledTask.objects.get(pk=pk)
        except Exception:
            return JsonResponse({}, status=405)
        import_name = scheduled_task.name.lower().replace(' ', '_')
        website_qs = Website.objects.filter(name=scheduled_task.name.split(' ')[0])
        if website_qs.exists():
            website = website_qs.first()
        else:
            website = None
        logger = FileLogHandler(website=website)
        if import_name in tasks.crawler_classes:
            logger.debug(f"{import_name} queued")
            tasks.run_crawler.delay(import_name)
        else:
            logger.debug(f"{import_name} not queued")
        return JsonResponse({}, status=200)
    else:
        return JsonResponse({}, status=405)


@login_required
def index(request):
    return render(request, "public/index.html")


@login_required
def get_websites(request):
    websites = Website.objects.all().values()
    data = json.dumps(list(websites), indent=4, sort_keys=True, default=str)
    return HttpResponse(data, content_type="application/json")



@login_required
def get_user(request):
    response = {"logged_in": False, "user": None}
    if request.user.is_authenticated:
        response = {"logged_in": True, "user": request.user.username}
    return JsonResponse(response, status=200)


@login_required
def show_website(request, website_id):
    if request.method == "GET":
        website = Website.objects.filter(pk=int(website_id))
        mapping_fields = website[0].website_mappings.all()
        locations = website[0].locationdetail_set.all() # Will be wrong
        response = {
            "website": list(website.values("name", "url", "category", "status", "id")),
            "mapping_fields": list(mapping_fields.values("system_field__name", "website_id", "id")),
            "locations": list(locations.values("system_code", "name", "website_id", "id")),
        }
        current_system_fields = [val["system_field__name"] for val in response["mapping_fields"]]
        response["system_fields"] = list(
            SystemField.objects.exclude(name__in=current_system_fields).values("name")
        )
        data = json.dumps(response, indent=4, sort_keys=True, default=str)
        return HttpResponse(data, content_type="application/json")
    else:
        return render(request, "public/index.html")

@login_required
def get_fields(request):
    fields = SystemField.objects.values("name", "action", "id")
    output = json.dumps(list(fields), indent=4, sort_keys=True, default=str)
    return HttpResponse(output, content_type="application/json")


@login_required
def get_mapping_field_values(request, mapping_field_id, website_id):
    if request.method == "GET":
        website_mapping_values = WebsiteMappingValue.objects.filter(
            mapping__website__id=int(website_id), mapping_id=int(mapping_field_id)
        ).values(
            "mapping__website_id",
            "mapping_id",
            "website_value",
            "system_value",
            "id",
        )
        website = (
            WebsiteMapping.objects.filter(id=int(mapping_field_id))
            .select_related("website")
            .values("website__name", "website_id", "id")
            .annotate(
                name=F("website__name"),
            )
        )
        response = {"website": list(website), "mapping_field_values": list(website_mapping_values)}
        output = json.dumps(response, indent=4, sort_keys=True, default=str)
        return HttpResponse(output, content_type="application/json")
    else:
        return render(request, "public/index.html")


@login_required
def save_mapping_field(request):
    if request.method == "POST":
        data = json.load(request)
        website = Website.objects.get(pk=int(data.get("websiteId")))
        system_field = SystemField.objects.filter(name=data.get("mappingFieldName"))
        if system_field.exists():
            website_mapping = WebsiteMapping(
                system_field=system_field.first(),
                website=website,
            )
            website_mapping.save()
            message = "Mapping Field added Successfully"
            response_data = {"status": 200, "response": "success", "message": message}
            return JsonResponse(response_data, status=200)
    return render(request, "public/405.html", status=405)


@login_required
def save_mapping_field_values(request):
    if request.method == "POST":
        data = json.load(request)
        if data.get("mappingFieldValueId"):
            WebsiteMappingValue.objects.filter(pk=int(data.get("mappingFieldValueId"))).update(
                system_value=data.get("systemValue"), website_value=data.get("websiteValue")
            )
            message = "Mapping Field Value updated successfully"
        else:
            website_mapping = WebsiteMapping.objects.get(pk=int(data.get("mappingFieldId")))
            website_mapping_value = WebsiteMappingValue(
                mapping=website_mapping,
                system_value=data.get("systemValue"),
                website_value=data.get("websiteValue"),
            )
            website_mapping_value.save()
            message = "Mapping Field Value saved Successfully"
        response_data = {"status": 200, "response": "success", "message": message}
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def delete_mapping_field_value(request, mapping_field_value_id):
    if request.method == "POST":
        WebsiteMappingValue.objects.filter(pk=int(mapping_field_value_id)).delete()
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Mapping Value deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def add_or_edit_website(request):
    if request.method == "POST":
        data = json.load(request)
        website_id = data.get("id")
        if website_id:
            Website.objects.filter(pk=website_id).update(
                category=data.get("category"),
                name=data.get("name"),
                status=data.get("status"),
                url=data.get("url"),
                comments=data.get("comments"),
            )
            message = "Website updated Successfully"
        else:
            message = "Website added Successfully"
            website = Website(
                category=data.get("category"),
                name=data.get("name"),
                url=data.get("url"),
                status=data.get("status"),
                comments=data.get("comments"),
            )
            website.save()
        response_data = {"status": 200, "response": "success", "message": message}
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def add_or_edit_field(request):
    if request.method == "POST":
        data = json.load(request)
        field_id = data.get("field_id")
        if field_id:
            SystemField.objects.filter(pk=int(field_id)).update(action=data.get("action"))
            message = "Field updated Successfully"
        else:
            message = "Field added Successfully"
            SystemField.objects.filter(name=data.get("name")).update(action=data.get("action"))
        response_data = {"status": 200, "response": "success", "message": message}
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def delete_field(request, field_id):
    if request.method == "POST":
        SystemField.objects.filter(pk=int(field_id)).update(action="")
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Field deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def add_or_edit_location(request):
    if request.method == "POST":
        data = json.load(request)
        location_id = data.get("locationId")
        if location_id:
            LocationDetail.objects.filter(pk=int(location_id)).update(
                name=data.get("locationName"),
                system_code=data.get("systemCode"),
            )
            message = "Location updated Successfully"
        else:
            message = "Location added Successfully"
            website = Website.objects.get(pk=int(data.get("websiteId")))
            location = LocationDetail(
                name=data.get("locationName"), system_code=data.get("systemCode"), website=website
            )
            location.save()

        response_data = {"status": 200, "response": "success", "message": message}
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)

@login_required
def delete_website(request, website_id):
    if request.method == "POST":
        Website.objects.filter(pk=website_id).delete()
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Website deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)

@login_required
def delete_schedule(request, pk):
    if request.method == "POST":
        ScheduledTask.objects.filter(pk=pk).delete()
        response_data = {
            "status": 200,
            "response": "success",
            "message": "ScheduledTask deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def delete_location(request, location_id):
    if request.method == "POST":
        LocationDetail.objects.filter(pk=int(location_id)).delete()
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Location deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def delete_mapping_field(request, mapping_field_id):
    if request.method == "POST":
        WebsiteMapping.objects.filter(pk=int(mapping_field_id)).delete()
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Mapping Field deleted Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


# @login_required
def edit_notifications(request):
    if request.method == "POST":
        data = json.load(request)
        Notification.objects.filter(name="webcrawlerReportEmails").update(
            email_list=data.get("webcrawlerReportEmails")
        )
        Notification.objects.filter(name="login_expiry").update(
            email_list=data.get("loginExpiryEmails"), day_of_week=data.get("loginExpiryNotifyTime")
        )
        Notification.objects.filter(name="webcrawler_down").update(
            email_list=data.get("websiteDownEmails"), day_of_week=data.get("websiteDownNotifyTime")
        )
        Notification.objects.filter(name="missing_mappings").update(
            email_list=data.get("missingMappingEmails")
        )
        Notification.objects.filter(name="bcc_mail").update(email_list=data.get("bccEmails"))
        response_data = {
            "status": 200,
            "response": "success",
            "message": "Notifications updated Successfully",
        }
        return JsonResponse(response_data, status=200)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def get_tracing_report(request):
    current_route = resolve(request.path_info).url_name
    if current_route in [
        "tracking-webcrawlers-excel-report",
        "tracking-webcrawlers-pdf-report",
        "tracking-unittraces-excel-report",
        "tracking-unittraces-pdf-report",
    ]:
        _to = request.GET.get("toDate", "")
        _from = request.GET.get("fromDate", "")
    elif request.method == "POST":
        return render(request, "public/405.html", status=405)
    else:
        _to = request.GET.get("toDate", "")
        _from = request.GET.get("fromDate", "")
    from_date = f"{_from} 00:00:00"
    to_date = f"{_to} 23:59:59"
    values = [
        "website_id__name",
        "website_id__status",
        "website_id",
        "website_id__category",
    ]

    if current_route in [
        "tracking-unittraces-report",
        "tracking-unittraces-pdf-report",
        "tracking-unittraces-excel-report",
    ]:
        values += ["units_traced", "success"]
        alias = {
            "name": F("website_id__name"),
            "status": F("website_id__status"),
            "category": F("website_id__category"),
            "created_at": Func(
                "created_at",
                Value("yyyy-mm-dd hh12:mi:ss AM"),
                function="to_char",
                output_field=CharField(),
            ),
            "failures": F("units_traced") - F("success"),
        }
        response = (
            TraceReportLog.objects.filter(created_at__lte=to_date, created_at__gte=from_date)
            .select_related("website_id")
            .values(*values)
            .annotate(**alias)
        )

    else:
        annotate = {
            "units_traced": Sum("units_traced"),
            "success": Sum("success"),
            "failures": F("units_traced") - F("success"),
            "created_at": Func(
                "created_at",
                Value("yyyy-mm-dd hh12:mi:ss AM"),
                function="to_char",
                output_field=CharField(),
            ),
            "name": F("website_id__name"),
            "status": F("website_id__status"),
            "category": F("website_id__category"),
        }
        response = (
            TraceReportLog.objects.filter(created_at__lte=to_date, created_at__gte=from_date)
            .select_related("website_id")
            .values(*values)
            .annotate(**annotate)
        )
    if current_route == "tracking-webcrawlers-excel-report":
        return generate_excel_report(response, report_type="webcrawler", to=_to, _from=_from)
    elif current_route == "tracking-unittraces-excel-report":
        return generate_excel_report(response, report_type="unittraces", to=_to, _from=_from)
    elif current_route in [
        "tracking-unittraces-pdf-report",
        "tracking-webcrawlers-pdf-report",
    ]:
        download_folder = settings.tracking_DOWNLOADS_DIR
        if current_route == "tracking-unittraces-pdf-report":
            filename = f"UnitTracesReport_{datetime.timestamp(datetime.now())}.pdf"
            report_name = "UnitTraces Report"
        else:
            filename = f"WebCrawlersReport_{datetime.timestamp(datetime.now())}.pdf"
            report_name = "WebCrawlers Report"
        template = get_template("public/pdf_reports.html")
        html = template.render(
            context={
                "items": response,
                "url": f"https://{request.get_host()}{settings.STATIC_URL}images/client_logo.png",
                "report_name": f"{report_name}: {_from} - {_to}",
            }
        )

        # create a pdf
        file = open(os.path.join(download_folder, filename), "w+b")
        pisa_status = pisa.CreatePDF(html, dest=file)
        with open(os.path.join(download_folder, filename), "rb") as file_path:
            response = HttpResponse(file_path.read(), content_type="application/pdf")
            response["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    return JsonResponse(list(response), safe=False, status=200)


def generate_excel_report(response, report_type="unittraces", to="", _from=""):
    columns = [
        "Name",
        "Website",
        "Website Status",
        "Last Traced",
        "Units Traced",
        "Successes",
        "Failures",
    ]
    # add custom color for header
    xlwt.add_palette_colour("header_color", 0x21)
    # Create a workbook and add a worksheet.
    workbook = xlwt.Workbook()
    # set custom header color
    workbook.set_colour_RGB(0x21, 226, 222, 208)
    if report_type == "unittraces":
        worksheet = workbook.add_sheet("UnitTraces Report")
    else:
        worksheet = workbook.add_sheet("WebCrawlers Report")
    # freeze first row
    worksheet.set_panes_frozen(True)
    worksheet.set_horz_split_pos(1)
    worksheet.set_vert_split_pos(1)
    # set font size to 12
    style = xlwt.easyxf(f"pattern: pattern solid,fore_colour white;font: name Calibri, height {12 * 20};")
    if report_type == "unittraces":
        header_column = f"UnitTraces Report: {_from} - {to}"
    else:
        header_column = f"WebCrawlers Report: {_from} - {to}"
    worksheet.write_merge(0, 0, 0, 5, header_column, style)
    # set font size to 12
    style = xlwt.easyxf(
        f"pattern: pattern solid, fore_colour header_color;font: name Calibri, bold True, height {12 * 20};"
    )
    for column_index, column in enumerate(columns):
        worksheet.col(column_index).width = 256 * (len(column) + 4)
        worksheet.write(2, column_index, column, style)

    # Start from the first cell below the headers.
    row = 3
    col = 0
    # set font size to 12
    style = xlwt.easyxf(f"font:color-index black, name Calibri, height {12 * 20}")

    for item in response:
        worksheet.write(row, col, "Imports", style)
        worksheet.write(row, col + 1, f"{item['name']}", style)
        worksheet.write(row, col + 2, f"{item['status']}", style)
        worksheet.write(row, col + 3, f"{item['created_at']}", style)
        worksheet.write(row, col + 4, f"{item['units_traced']}", style)
        worksheet.write(row, col + 5, f"{item['success']}", style)
        worksheet.write(row, col + 6, f"{item['units_traced'] - item['success']}", style)
        row += 1
    if report_type == "unittraces":
        filename = f"UnitTracesReport_{datetime.timestamp(datetime.now())}.xls"
    else:
        filename = f"WebCrawlersReport_{datetime.timestamp(datetime.now())}.xls"

    download_folder = settings.tracking_DOWNLOADS_DIR
    workbook.save(f"{os.path.join(download_folder, filename)}")
    with open(os.path.join(download_folder, filename), "rb") as file_path:
        response = HttpResponse(file_path.read(), content_type="application/ms-excel")
        response["Content-Disposition"] = f"attachment; filename={filename}"
        return response


@login_required
def get_recent_traces(request):
    if request.method == "GET":
        timeZ = request.GET.get('timeZ', '')
        to_date = f"{date.today()} 23:59:59"
        from_date = f"{date.today() + timedelta(days=-3)} 00:00:00"
        websites = Website.objects.all()
        website_statuses = websites.values("status").annotate(status_count=Count("status"))
        alias = {
            "name": F("website__name"),
            "status": F("website__status"),
            "category": F("website__category"),
            "last_traced": F('created_at')
        }
        recent_traces = (
            TraceReportLog.objects.filter(created_at__lte=to_date, created_at__gte=from_date)
            .select_related("website_id")
            .values(
                "website__name",
                "website__status",
                "units_traced",
                "website",
                "success",
                "website__category",
            )
            .annotate(**alias)
            .order_by("-created_at")
        )
        for result in recent_traces:
             result['last_traced'] = timezone.localtime(result['last_traced'], pytz.timezone(timeZ)).strftime('%Y-%m-%d %I:%M:%S %p')
        response = {"website_statuses": list(website_statuses), "recent_traces": list(recent_traces)}
        return JsonResponse(response, safe=False, status=200)
    else:
        return render(request, "public/405.html", status=405)

@login_required
def get_notifications(request):
    if request.method == "GET":
        notifications_list = Notification.objects.values()
        return JsonResponse(list(notifications_list), safe=False, status=200)
    else:
        return render(request, "public/405.html", status=405)

@login_required
def get_reports(request):
    return render(request, "tracking/report.html")

@login_required
def get_tracing_schedules(request):

    scheduled_tasks = ScheduledTask.objects.filter(category__in=['Import', 'Export', 'Other'])
    objs = []
    for task in scheduled_tasks:
        cron = task.celery_task.crontab
        obj = {
            'id': task.pk,
            'name': task.name,
            'frequency': task.frequency,
            'category': task.category,
            'start_time': get_cron_start_time(cron).strftime('%I:%M %p'),
            'end_time': get_cron_end_time(cron).strftime('%I:%M %p')
        }
        objs.append(obj)
    data = json.dumps(list(objs), indent=4, sort_keys=True, default=str)
    return HttpResponse(data, content_type="application/json")


@login_required
def add_or_edit_schedule(request):
    if request.method == "POST":
        data = json.load(request)
        pk = data.get("id")
        if pk:
            scheduled_task = ScheduledTask.objects.filter(pk=pk).first()
            scheduled_task.category = data.get('category')

            celery_task = scheduled_task.celery_task
            celery_task.name = data.get('name')

            cron = celery_task.crontab
            start_hour, start_minute = data.get('start_time', ':').split(':')
            end_hour, end_minute = data.get('end_time', ':').split(':')
            cron.minute = f'{start_minute}-{end_minute}'
            cron.hour = f'{start_hour}-{end_hour}'

            frequency = data.get('frequency', 'daily')
            scheduled_task.frequency = frequency
            cron.day_of_week = '*'
            cron.day_of_month = '*'
            if frequency == 'Weekly':
                # Runs every Monday
                cron.day_of_week = '1'
            elif frequency == 'Monthly':
                # Runs on first day of every month
                cron.day_of_month = '1'

            cron.save()
            celery_task.save()
            scheduled_task.save()

            message = "Schedule updated Successfully!"
            response_data = {"status": 200, "response": "success", "message": message}
            return JsonResponse(response_data, status=200)
        return HttpResponse(status=500)
    else:
        return render(request, "public/405.html", status=405)


@login_required
def get_report_schedules(request):
    scheduled_tasks = ScheduledTask.objects.filter(category__in=['Email'])
    objs = []
    for task in scheduled_tasks:
        cron = task.celery_task.crontab
        cron_start_time = time(int(cron.hour), int(cron.minute))
        data_dict = json.loads(task.celery_task.kwargs)
        start_date = data_dict.get('start_date')
        end_date = data_dict.get('end_date')

        obj = {
            'id': task.pk,
            'name': task.name,
            'frequency': task.frequency,
            'category': task.category,
            'email_list': task.email_list,
            'delivery_time': cron_start_time.strftime('%I:%M %p') if cron_start_time else '',
            'date_range': f'{start_date} - {end_date}',
            'date_range_start': start_date,
            'date_range_end': end_date,
            'format': task.format,
            'report_type': data_dict.get('report_type'),
        }
        objs.append(obj)
    data = json.dumps(list(objs), indent=4, sort_keys=True, default=str)
    return HttpResponse(data, content_type="application/json")

@login_required
def add_or_edit_report_schedule(request):
    if request.method == 'POST':
        body = json.load(request)
        pk = body.get('id')
        json_kwargs = {
            'email_list': body.get('email_list', ''),
            'format': body.get('format', ''),
            'start_date': body.get('date_range_start'),
            'end_date': body.get('date_range_end'),
            'report_type': body.get('report_type'),
        }
        if pk:
            # update
            scheduled_task = ScheduledTask.objects.filter(pk=pk).first()

            celery_task = scheduled_task.celery_task
            # Update format, and email_list
            celery_task.kwargs = json.dumps(json_kwargs)

            cron = celery_task.crontab

            # Update cron with delivery time
            hour, minute = body.get('delivery_time', ':').split(':') # the time for the report to be sent
            cron.minute = int(minute)
            cron.hour = int(hour)

            # Update cron with frequency
            frequency = body.get('frequency', 'daily')
            scheduled_task.frequency = frequency
            cron.day_of_week = '*'
            cron.day_of_month = '*'
            if frequency == 'Weekly':
                # Runs every Monday
                cron.day_of_week = '1'
            elif frequency == 'Monthly':
                # Runs on first day of every month
                cron.day_of_month = '1'

            celery_task.name = body.get('name')
            scheduled_task.save()
            cron.save()
            celery_task.save()
        else:
            # Create crontab
            cron = CrontabSchedule()
            # Update cron with delivery time
            hour, minute = body.get('delivery_time', ':').split(':')
            cron.minute = int(minute)
            cron.hour = int(hour)

            # Update cron with frequency
            frequency = body.get('frequency', 'daily')
            cron.day_of_week = '*'
            cron.day_of_month = '*'
            if frequency == 'Weekly':
                # Runs every Monday
                cron.day_of_week = '1'
            elif frequency == 'Monthly':
                # Runs on first day of every month
                cron.day_of_month = '1'
            cron.save()

            # create
            celery_task = PeriodicTask.objects.create(
                name=body.get('name'),
                start_time=datetime.now(),
                expire_seconds=300, # 5 minutes
                crontab=cron,
                kwargs=json.dumps(json_kwargs),
            )
            ScheduledTask.objects.create(
                celery_task=celery_task,
                category='Email',
                frequency=frequency,
            )
        response_data = {"status": 200, "response": "success", "message": "Schedule updated successfully"}
        return JsonResponse(response_data, status=200)
    return HttpResponse(status=405)
