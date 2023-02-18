import asyncio
import os
import datetime
import json
import django
import time

from channels.layers import get_channel_layer

from confluent_kafka.avro import Consumer
from confluent_kafka.avro.serializer import SerializerError
from django.utils.timezone import make_aware

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")
django.setup()

from exhauster_analytics.analytics.models import (
    ExgausterSignal,
    Record,
    ExgausterRecordSignal,
    RecordApproximation,
    ExgausterRecordApproximationSignal,
)
from config.settings.base import structlog

log = structlog.get_logger()

print("loading values from DB, please wait")

values = {}
for sign in Record.objects.last().signals.all():
    values[sign.signal.name] = []

em = len(Record.objects.all())
if em > 60:
    for record in Record.objects.all()[em - 60 :]:
        for sign in record.signals.all():
            if sign.signal.name in values:
                values[sign.signal.name].append(sign.value)


conf = {
    "bootstrap.servers": "rc1a-2ar1hqnl386tvq7k.mdb.yandexcloud.net:9091",
    "group.id": "MentalMind",
    "session.timeout.ms": 6000,
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "CA.pem",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "9433_reader",
    "sasl.password": "eUIpgWu0PWTJaTrjhjQD3.hoyhntiK",
    "auto.offset.reset": "end",
}


def my_assign(consumer, partitions):
    for p in partitions:
        p.offset = Record.objects.last().offset + 1
    print("assign", partitions)
    consumer.assign(partitions)


async def send_to_channel_layer(data):
    channel_layer = get_channel_layer()
    await channel_layer.group_send("notifications", {"data": data, "type": "info"})


async def send_to_channel_layer_approximation(data, approximation):
    channel_layer = get_channel_layer()
    await channel_layer.group_send(
        f"approximation_{approximation}", {"data": data, "type": "info"}
    )


c = Consumer(conf)

c.subscribe(["zsmk-9433-dev-01"], on_assign=my_assign)

signals = {}
for ex in ExgausterSignal.objects.all():
    if ex.place_x not in signals:
        signals[ex.place_x] = {}
    if ex.place_y not in signals[ex.place_x]:
        signals[ex.place_x][ex.place_y] = {}
    signals[ex.place_x][ex.place_y][ex.type] = ex

# delete latest offset
offset = Record.objects.last().offset
Record.objects.filter(offset=offset).delete()
RecordApproximation.objects.filter(offset=offset).delete()

while True:
    try:
        msg = c.poll(10)
    except SerializerError as e:
        log.info("Message deserialization failed for {}: {}".format(msg, e))
        continue

    if msg is None:
        continue

    if msg.error():
        log.info("AvroConsumer error: {}".format(msg.error()))
        continue
    try:
        data = json.loads(msg.value())
    except json.JSONDecodeError:
        log.info("Message deserialization failed for {}".format(msg))
        continue
    date = make_aware(
        datetime.datetime.strptime(data["moment"], "%Y-%m-%dT%H:%M:%S.%f")
        + datetime.timedelta(hours=3)
    )
    offset = msg.offset()
    start = time.time()
    print(f"DEBUG: received {offset}, {date}")
    loop = asyncio.get_event_loop()
    coroutine = send_to_channel_layer(data)
    loop.run_until_complete(coroutine)
    record = Record.objects.create(timestamp=date, offset=offset, message=data)
    if offset != 0:
        approximation_amounts = [
            x for x, _ in RecordApproximation.AmountChoices.choices if offset % x == 0
        ]
    else:
        approximation_amounts = []
    approximation = {}
    for amount in approximation_amounts:
        approximation[amount] = RecordApproximation.objects.create(
            amount=amount, timestamp=date, offset=offset
        )
    approximation_values = {}
    for approximation_val in approximation_amounts:
        approximation_values[approximation_val] = {}

    for key, val in data.items():
        if "SM" in key:
            if key in values:
                del values[key][0]
            else:
                values[key] = [0] * 60
            values[key].append(val)

            if "." in key:
                x, y = map(int, key[key.find("[") + 1 : key.find("]")].split("."))
                type = "digital"
            else:
                x, y = map(int, key[key.find("[") + 1 : key.find("]")].split(":"))
                type = "analog"
            try:
                signal = signals[x][y][type]
                ExgausterRecordSignal.objects.create(
                    record=record, signal=signal, value=val
                )
            except KeyError:
                continue
            for amount, approx in approximation.items():
                vals = values[key][60-amount:]
                r = sum(vals) / len(vals)
                approximation_values[amount][key] = r
                ExgausterRecordApproximationSignal.objects.create(
                    record=approx, signal=signal, value=r
                )
    for approx, data in approximation_values.items():
        loop = asyncio.get_event_loop()
        coroutine = send_to_channel_layer_approximation(data, approx)
        loop.run_until_complete(coroutine)

    print(f"DEBUG: done {offset}, {time.time() - start} sec taken")

c.close()
