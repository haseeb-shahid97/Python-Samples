import dateutil.parser as parser
from datetime import datetime, time, date
from strict_hint import strict
import logging
import xlwt
import os
import json
from xhtml2pdf import pisa
from django.conf import settings
import socket
from django.template.loader import get_template

logger = logging.getLogger(__name__)


# case insensitive string comparison
@strict
def stri_compare(str1, str2) -> bool:
    if isinstance(str1, str) and isinstance(str2, str):
        return str1.upper().strip() == str2.upper().strip()
    else:
        return str1 == str2


@strict
def convert_string_into_db2_format(string) -> str:
    if string is None or string == "":
        return ""
    try:
        return parser.parse(string).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as ex:
        logger.info(
            f"Error while trying to convert string {string} into db2 format, {ex}"
        )
    return str(string)


@strict
def format_date(
        date_time,
        date_format="%m/%d/%Y",
        return_value="",
) -> str:
    formatted_date = return_value
    try:
        if isinstance(date_time, str):
            formatted_date = parser.parse(date_time).strftime(date_format)
        else:
            formatted_date = date_time.strftime(date_format)
    except Exception as ex:
        logger.info(
            f"formatted date exception {date_time}, {ex}"
        )
    return formatted_date


@strict
def is_null_or_empty(val) -> bool:
    if val is None or val.strip() == "":
        return True
    return False


def calculate_seconds(start_time: str, end_time: str):
    start_hour, start_min = start_time.split(':')
    end_hour, end_min = end_time.split(':')

    start_hour, end_hour, start_min, end_min = int(start_hour), int(end_hour), int(start_min), int(end_min)
    duration = datetime.combine(date.today(), time(end_hour, end_min)) - datetime.combine(date.today(),
                                                                                          time(start_hour, start_min))
    return duration.seconds


def get_cron_start_time(cron):
    minute = cron.minute
    hour = cron.hour
    if '-' in cron.minute:
        minute = cron.minute.split('-')[0]
    if '-' in cron.hour:
        hour = cron.hour.split('-')[0]
    return time(int(hour), int(minute))


def get_cron_end_time(cron):
    minute = cron.minute
    hour = cron.hour
    if '-' in cron.minute:
        minute = cron.minute.split('-')[1]
    if '-' in cron.hour:
        hour = cron.hour.split('-')[1]
    return time(int(hour), int(minute))


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
    return os.path.join(download_folder, filename)


def generate_pdf_report(items, report_name, start_date, end_date):
    filename = report_name.replace(' ', '') + f"_{datetime.timestamp(datetime.now())}.pdf"
    template = get_template("public/pdf_reports.html")
    html = template.render(
        context={
            "items": items,
            "url": f"https://{socket.gethostname()}{settings.STATIC_URL}images/client_logo.png",
            "report_name": f"{report_name}: {start_date} - {end_date}",
        }
    )

    file = open(os.path.join(settings.tracking_DOWNLOADS_DIR, filename), "w+b")
    pisa_status = pisa.CreatePDF(html, dest=file)
    file.close()
    return os.path.join(settings.tracking_DOWNLOADS_DIR, filename)


def get_gpa_current_status(available, location):
    if available.upper() == 'YES':
        return 'RELEASED'
    elif available.upper() == 'NO' and location.upper() == 'Y':
        return 'HOLD'
    else:
        return ''


def get_gpa_hold(available, outgated, line_status, custom_status, other_holds):
    if available.upper() == "YES" and outgated != "":
        return 'HOLD'
    elif line_status.upper() != 'RELEASED':
        return 'LINE HOLD'
    elif custom_status.upper() != 'RELEASED':
        return 'CUSTOM HOLD'
    elif other_holds.upper() != 'N':
        return 'OTHER HOLD'
