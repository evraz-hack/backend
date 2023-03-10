import json

from asgiref.sync import sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from exhauster_analytics.analytics.models import Record


class NotificationsConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.room_group_name = None

    async def connect(self):
        self.room_group_name = "notifications"
        await self.accept()

        data = await self.get_last_record()
        await self.send(text_data=json.dumps(data))

        await self.channel_layer.group_add(self.room_group_name, self.channel_name)

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    # Receive message from WebSocket
    async def receive(self, text_data):
        pass

    @sync_to_async
    def get_last_record(self):
        data = {}
        record = Record.objects.last()
        for signal in record.signals.all():
            if not signal.signal.config:
                data[signal.signal.name] = signal.value
                if signal.signal.installations:
                    data[f"{signal.signal.name}_status"] = "normal"

        return data

    async def info(self, event):
        message = event["data"]

        await self.send(text_data=json.dumps(message))


class NotificationsApproximateConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.room_group_name = None
        self.room_name = None

    async def connect(self):
        self.room_group_name = "notifications"
        approximation = self.scope["url_route"]["kwargs"]["approximation"]
        if approximation not in [10, 30, 60]:
            await self.close()
        self.room_name = approximation
        self.room_group_name = f"approximation_{approximation}"

        await self.accept()
        await self.channel_layer.group_add(self.room_group_name, self.channel_name)

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    # Receive message from WebSocket
    async def receive(self, text_data):
        pass

    async def info(self, event):
        message = event["data"]

        await self.send(text_data=json.dumps(message))
