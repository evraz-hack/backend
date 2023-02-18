from . import consumers
from django.urls import path

websocket_urlpatterns = [
    path("", consumers.NotificationsConsumer.as_asgi()),
    path("<int:approximation>", consumers.NotificationsApproximateConsumer.as_asgi()),
]
