from django.urls import path

from exhauster_analytics.analytics.api.views import (
    ListExgauster,
    GetApproximatedExgausterSignals,
)

app_name = "analytics"
urlpatterns = [
    path("list", ListExgauster.as_view()),
    path("approximation", GetApproximatedExgausterSignals.as_view()),
]
