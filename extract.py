import os
import datetime
import json
import django
import csv

from confluent_kafka.avro import Consumer
from confluent_kafka.avro.serializer import SerializerError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.local")
django.setup()

from exhauster_analytics.analytics.models import (
    Exgauster,
)
from config.settings.base import structlog

log = structlog.get_logger()

conf = {
    "bootstrap.servers": "rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091",
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
        p.offset = 0
    print("assign", partitions)
    consumer.assign(partitions)



for n in range(1, 7):
    c = Consumer(conf)
    c.subscribe(["zsmk-9433-dev-01"], on_assign=my_assign)
    signals = []
    for ex in Exgauster.objects.get(number=n).signals.all():
        signals.append(
            f"SM_Exgauster\\[{ex.place_x}{':' if ex.type == 'analog' else '.'}{ex.place_y}]"
        )

    res = []

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

        rows = []
        date = datetime.datetime.strptime(
            data["moment"], "%Y-%m-%dT%H:%M:%S.%f"
        ) + datetime.timedelta(hours=3)
        for key in signals:
            try:
                rows.append(data[key])
            except KeyError:
                try:
                    rows.append(data[key.replace(":", ".")])
                except KeyError:
                    try:
                        rows.append(data[key.replace(".", ":")])
                    except KeyError:
                        rows.append(None)

        res.append([date] + rows)

        if msg.offset() % 100 == 0:
            print(msg.offset())

        if msg.offset() > 31500:
            break

    signals = []
    for ex in Exgauster.objects.get(number=1).signals.all():
        signals.append(
            f"SM_Exgauster\[{ex.place_x}{':' if ex.type == 'analog' else '.'}{ex.place_y}]"
        )

    with open(f"{n}.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp"] + signals)
        for row in res:
            writer.writerow(row)

    c.close()
