from celery import shared_task

from datetime import datetime
from tracking.models import ScheduledTask, TraceReportLog
from tracking.helpers import generate_excel_report, generate_pdf_report
from django.db.models import Sum,  Value, CharField, Func, F
from django.core.mail import EmailMessage

from tracking.Imports.bct_imports import BCTImports
from tracking.Imports.bpt_imports import BPTImports
from tracking.Imports.csx_imports import CSXImports
from tracking.Imports.csx_nashville_imports import CSXImports
from tracking.Imports.vaports_imports import VAPortsImports
from tracking.Imports.gpa_imports import GPAImports
from tracking.Imports.nsrr_imports import NSRRImports
from tracking.Imports.sgrt_imports import SGRTImports
from tracking.Imports.uprr_imports import UPRRImports
from tracking.Imports.cn_imports import CNImports
from tracking.Imports.sc_imports import SCImports
from tracking.Imports.bnsf_imports import BNSFImports


crawler_classes = {
    "bct_imports": BCTImports,
    "bpt_imports": BPTImports,
    "csx_imports": CSXImports,
    "csx_nashville_imports": CSXImports,
    "vaports_imports": VAPortsImports,
    "gpa_imports": GPAImports,
    "nsrr_imports": NSRRImports,
    "sgrt_imports": SGRTImports,
    "uprr_imports": UPRRImports,
    "cn_imports": CNImports,
    "sc_imports": SCImports,
    "bnsf_imports": BNSFImports,
}


@shared_task(soft_time_limit=2500,time_limit=2500)
def run_crawler(crawler: str):
    global crawler_classes
    if crawler in crawler_classes:
        crawler_instance = crawler_classes[crawler]()
        crawler_instance.run()
    else:
        raise Exception("Invalid Crawler Name")


@shared_task
def scheduled_task_cleanup():
    ScheduledTask.objects.filter(disable_datetime__lte=datetime.now(), celery_task__enabled=True).update(celery_task__enabled=False)


@shared_task
def email_report(email_list, format, report_type, start_date, end_date):
    # Don't need the task lol
    report_name = ''
    items = ()
    values = [
        "website_id__name",
        "website_id__status",
        "website_id",
        "website_id__category",
    ]
    if report_type == 'UnitTraces':
        report_name = 'UnitTraces Report'
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
        items = (
            TraceReportLog.objects.filter(created_at__lte=start_date, created_at__gte=end_date)
            .select_related("website_id")
            .values(*values)
            .annotate(**alias)
        )
    else:
        report_name = 'WebCrawlers Report'
        alias = {
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
        items = (
            TraceReportLog.objects.filter(created_at__lte=start_date, created_at__gte=end_date)
            .select_related("website_id")
            .values(*values)
            .annotate(**alias)
        )

    file_path = ''
    if format == 'PDF':
        file_path = generate_pdf_report(items, report_name, start_date, end_date)
    else:
        format = 'Excel'
        file_path = generate_excel_report(items, report_name, start_date, end_date)

    email_msg = EmailMessage(
        subject=f'[tracking] {report_name} - {format}',
        body='The report is attached below.',
        from_email="dun.system.messages@client.com",
        to=email_list.split(','),
    )
    email_msg.attach_file(file_path)
    email_msg.send()
