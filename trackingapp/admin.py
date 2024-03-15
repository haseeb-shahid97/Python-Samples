from django.contrib import admin
from django.db.models import Avg, F
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from tracking.models import (
    LocationDetail,
    Notification,
    Request,
    RequestKPI,
    ScacCodes,
    ScheduledTask,
    SystemField,
    TraceReportLog,
    TrackingErrorLog,
    Website,
    WebsiteMapping,
    WebsiteMappingValue,
    UPRRToken
)

admin.site.register(Notification)
admin.site.register(Website)
admin.site.register(TraceReportLog)
admin.site.register(LocationDetail)
admin.site.register(SystemField)
admin.site.register(WebsiteMapping)
admin.site.register(WebsiteMappingValue)
admin.site.register(ScheduledTask)
admin.site.register(Request)
admin.site.register(ScacCodes)
admin.site.register(UPRRToken)


class TrackingErrorLogAdmin(admin.ModelAdmin):
    readonly_fields = (
        "created_at",
        "subject",
        "message",
        "website",
        "reference_val",
        "reference_type",
    )

    list_display = (
        "website",
        "subject",
        "message",
        "status",
        "level",
        "created_at",
    )

    list_filter = (
        "status",
        "level",
        "website",
    )

    search_fields = ["website", "subject", "message", "reference_val"]


admin.site.register(TrackingErrorLog, TrackingErrorLogAdmin)


class WebsiteNameFilter(admin.SimpleListFilter):
    title = _("Website Name")
    parameter_name = "website_name"

    def lookups(self, request, model_admin):
        websites = Website.objects.values_list("name", flat=True)
        return [(website, website) for website in websites]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(website__name=self.value())
        else:
            return queryset


class DateFilter(admin.SimpleListFilter):
    title = _("Date Filter")
    parameter_name = "date"

    def lookups(self, request, model_admin):
        return [
            ("Todays", "Todays"),
            ("Past 7 days", "Past 7 days"),
            ("This month", "This month"),
            ("This year", "This year"),
        ]

    def queryset(self, request, queryset):
        return queryset


class RequestKPIAdmin(admin.ModelAdmin):
    list_filter = (WebsiteNameFilter, DateFilter)
    list_per_page = 999999
    change_list_template = "tracking/custom_kpi_template.html"

    def get_queryset(self, request):
        qs = super().get_queryset(request)

        if request.GET.getlist("website_name"):
            return qs.filter(website__name=request.GET.getlist("website_name")[0])
        elif request.GET.getlist("date"):
            today = timezone.now().date()
            if request.GET.getlist("date")[0] == "Todays":
                return qs.filter(created_at__date=today)
            elif request.GET.getlist("date")[0] == "Past 7 days":
                seven_days_ago = today - timezone.timedelta(days=7)
                return qs.filter(created_at__date__range=[seven_days_ago, today])
            elif request.GET.getlist("date")[0] == "This month":
                one_month_ago = today - timezone.timedelta(days=30)
                return qs.filter(created_at__date__range=[one_month_ago, today])
            elif request.GET.getlist("date")[0] == "This year":
                one_year_ago = today - timezone.timedelta(days=365)
                return qs.filter(created_at__date__range=[one_year_ago, today])
        return qs

    def count_get_request_dunt_to_tmdb(self, obj):
        return obj.filter(sender="tracking", receiver="TMDB", method="GET").count()

    def count_post_request_dunt_to_api(self, obj):
        return obj.filter(
            sender="tracking", receiver=obj.first().website.name, method="POST"
        ).count()

    def count_post_request_dunt_to_tmdb(self, obj):
        return obj.filter(sender="tracking", receiver="TMDB", method="POST").count()

    # Count of containers pulled from TMDB
    def containers_pulled_from_tmdb(self, obj):
        queryset = obj.filter(sender="tracking", receiver="TMDB", method="GET")
        return sum(int(obj.containers or 0) for obj in queryset)

    # Count of containers data from Vaports
    def containers_data_get_from_api(self, obj):
        queryset = obj.filter(
            sender="tracking", receiver=obj.first().website.name, method="POST"
        )
        return sum(int(obj.containers or 0) for obj in queryset)

    # Count of containers pushed to TMDB
    def containers_pushed_to_tmdb(self, obj):
        queryset = obj.filter(sender="tracking", receiver="TMDB", method="POST")
        return sum(int(obj.containers or 0) for obj in queryset)

    # Get Overall average
    def get_overall_average_time(self, obj):
        return round(
            obj.aggregate(average_time_diff=Avg(F("stop_time") - F("start_time")))[
                "average_time_diff"
            ].total_seconds(),
            2,
        )

    # Get Overall average
    def get_last_average_time(self, obj):
        first_object = (
            obj.filter(sender="tracking", receiver="TMDB", method="GET")
            .last()
            .start_time
        )
        last_object = (
            obj.filter(sender="tracking", receiver="TMDB", method="POST")
            .last()
            .stop_time
        )
        try:
            total_seconds = round((last_object - first_object).total_seconds(), 2)
        except:
            total_seconds = "Calculating"
        return total_seconds

    # First Leg RoundTrip
    def get_first_leg_average_time(self, obj):
        return round(
            obj.filter(sender="tracking", receiver="TMDB", method="GET")
            .aggregate(average_time_diff=Avg(F("stop_time") - F("start_time")))[
                "average_time_diff"
            ]
            .total_seconds(),
            2,
        )

    # Second Leg RoundTrip
    def get_second_leg_average_time(self, obj):
        return round(
            obj.filter(
                sender="tracking", receiver=obj.first().website.name, method="POST"
            )
            .aggregate(average_time_diff=Avg(F("stop_time") - F("start_time")))[
                "average_time_diff"
            ]
            .total_seconds(),
            2,
        )

    # Third Leg RoundTrip
    def get_third_leg_average_time(self, obj):
        return round(
            obj.filter(sender="tracking", receiver="TMDB", method="POST")
            .aggregate(average_time_diff=Avg(F("stop_time") - F("start_time")))[
                "average_time_diff"
            ]
            .total_seconds(),
            2,
        )

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["show_full_result_count"] = False
        obj = self.get_queryset(request)

        extra_context["get_dt_tmdb_count"] = self.count_get_request_dunt_to_tmdb(obj)
        extra_context["post_dt_vp_count"] = self.count_post_request_dunt_to_api(obj)
        extra_context["post_dt_tmdb_count"] = self.count_post_request_dunt_to_tmdb(obj)

        extra_context["containers_pulled_from_tmdb"] = self.containers_pulled_from_tmdb(
            obj
        )
        extra_context[
            "containers_data_get_from_api"
        ] = self.containers_data_get_from_api(obj)
        extra_context["containers_pushed_to_tmdb"] = self.containers_pushed_to_tmdb(obj)

        extra_context["get_overall_average_time"] = self.get_overall_average_time(obj)
        extra_context["get_last_average_time"] = self.get_last_average_time(obj)

        extra_context["get_first_leg_average_time"] = self.get_first_leg_average_time(
            obj
        )
        extra_context["get_second_leg_average_time"] = self.get_second_leg_average_time(
            obj
        )
        extra_context["get_third_leg_average_time"] = self.get_third_leg_average_time(
            obj
        )

        return super().changelist_view(request, extra_context=extra_context)


admin.site.register(RequestKPI, RequestKPIAdmin)
