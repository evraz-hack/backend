from django.urls import path, include

app_name = "api"
urlpatterns = [
    path("exgausters/", include("analytics.api.urls"))
]
