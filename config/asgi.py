import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")

django_asgi_app = get_asgi_application()

from exhauster_analytics.analytics.routing import websocket_urlpatterns

application = ProtocolTypeRouter(
    {"http": django_asgi_app, "websocket": URLRouter(websocket_urlpatterns)}
)
