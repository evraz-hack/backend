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

print("loading values from DB, please wait")

# from inference import run

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
values = {}
for sign in ExgausterSignal.objects.all():
    if not sign.config:
        values[sign.name] = []

em = len(Record.objects.all())
if em > 60:
    values["moment"] = [x.timestamp for x in Record.objects.all()[em - 60 :]]
    for record in Record.objects.all()[em - 60 :]:
        for sign in record.signals.all():
            if not sign.signal.config and sign.signal.name in values:
                values[sign.signal.name].append(sign.value)


statuses = {}
keys_not_to_send = []

for signal in ExgausterSignal.objects.filter(config__isnull=True):
    if signal.characteristics_description:
        statuses[signal.name] = {}
        for installation in signal.installations.all():
            keys_not_to_send.append(installation.name)
            statuses[signal.name][installation.item_name] = installation.name

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
mapping = {
    "SM_Exgauster[2:27]": "Подшипник 1 Температура нагрева Температура temperature",
    "SM_Exgauster[2:65]": "Подшипник 1 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:74]": "Подшипник 1 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:83]": "Подшипник 1 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:92]": "Подшипник 1 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:2]": "Подшипник 1 Вибрация Осевая vibration_axial",
    "SM_Exgauster[2:139]": "Подшипник 1 Вибрация Осевая alarm_max",
    "SM_Exgauster[2:151]": "Подшипник 1 Вибрация Осевая alarm_min",
    "SM_Exgauster[2:163]": "Подшипник 1 Вибрация Осевая warning_max",
    "SM_Exgauster[2:175]": "Подшипник 1 Вибрация Осевая warning_min",
    "SM_Exgauster[2:0]": "Подшипник 1 Вибрация Горизонтальная vibration_horizontal",
    "SM_Exgauster[2:137]": "Подшипник 1 Вибрация Горизонтальная alarm_max",
    "SM_Exgauster[2:149]": "Подшипник 1 Вибрация Горизонтальная alarm_min",
    "SM_Exgauster[2:161]": "Подшипник 1 Вибрация Горизонтальная warning_max",
    "SM_Exgauster[2:173]": "Подшипник 1 Вибрация Горизонтальная warning_min",
    "SM_Exgauster[2:1]": "Подшипник 1 Вибрация Вертикальная vibration_vertical",
    "SM_Exgauster[2:138]": "Подшипник 1 Вибрация Вертикальная alarm_max",
    "SM_Exgauster[2:150]": "Подшипник 1 Вибрация Вертикальная alarm_min",
    "SM_Exgauster[2:162]": "Подшипник 1 Вибрация Вертикальная warning_max",
    "SM_Exgauster[2:174]": "Подшипник 1 Вибрация Вертикальная warning_min",
    "SM_Exgauster[2:28]": "Подшипник 2 Температура нагрева Температура temperature",
    "SM_Exgauster[2:66]": "Подшипник 2 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:75]": "Подшипник 2 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:84]": "Подшипник 2 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:93]": "Подшипник 2 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:5]": "Подшипник 2 Вибрация Осевая vibration_axial",
    "SM_Exgauster[2:142]": "Подшипник 2 Вибрация Осевая alarm_max",
    "SM_Exgauster[2:154]": "Подшипник 2 Вибрация Осевая alarm_min",
    "SM_Exgauster[2:166]": "Подшипник 2 Вибрация Осевая warning_max",
    "SM_Exgauster[2:178]": "Подшипник 2 Вибрация Осевая warning_min",
    "SM_Exgauster[2:3]": "Подшипник 2 Вибрация Горизонтальная vibration_horizontal",
    "SM_Exgauster[2:140]": "Подшипник 2 Вибрация Горизонтальная alarm_max",
    "SM_Exgauster[2:152]": "Подшипник 2 Вибрация Горизонтальная alarm_min",
    "SM_Exgauster[2:164]": "Подшипник 2 Вибрация Горизонтальная warning_max",
    "SM_Exgauster[2:176]": "Подшипник 2 Вибрация Горизонтальная warning_min",
    "SM_Exgauster[2:4]": "Подшипник 2 Вибрация Вертикальная vibration_vertical",
    "SM_Exgauster[2:141]": "Подшипник 2 Вибрация Вертикальная alarm_max",
    "SM_Exgauster[2:153]": "Подшипник 2 Вибрация Вертикальная alarm_min",
    "SM_Exgauster[2:165]": "Подшипник 2 Вибрация Вертикальная warning_max",
    "SM_Exgauster[2:177]": "Подшипник 2 Вибрация Вертикальная warning_min",
    "SM_Exgauster[2:29]": "Подшипник 3 Температура нагрева Температура temperature",
    "SM_Exgauster[2:67]": "Подшипник 3 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:76]": "Подшипник 3 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:85]": "Подшипник 3 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:94]": "Подшипник 3 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:30]": "Подшипник 4 Температура нагрева Температура temperature",
    "SM_Exgauster[2:68]": "Подшипник 4 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:77]": "Подшипник 4 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:86]": "Подшипник 4 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:95]": "Подшипник 4 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:31]": "Подшипник 5 Температура нагрева Температура temperature",
    "SM_Exgauster[2:69]": "Подшипник 5 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:78]": "Подшипник 5 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:87]": "Подшипник 5 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:96]": "Подшипник 5 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:32]": "Подшипник 6 Температура нагрева Температура temperature",
    "SM_Exgauster[2:70]": "Подшипник 6 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:79]": "Подшипник 6 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:88]": "Подшипник 6 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:97]": "Подшипник 6 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:33]": "Подшипник 7 Температура нагрева Температура temperature",
    "SM_Exgauster[2:71]": "Подшипник 7 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:80]": "Подшипник 7 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:89]": "Подшипник 7 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:98]": "Подшипник 7 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:8]": "Подшипник 7 Вибрация Осевая vibration_axial",
    "SM_Exgauster[2:145]": "Подшипник 7 Вибрация Осевая alarm_max",
    "SM_Exgauster[2:157]": "Подшипник 7 Вибрация Осевая alarm_min",
    "SM_Exgauster[2:169]": "Подшипник 7 Вибрация Осевая warning_max",
    "SM_Exgauster[2:181]": "Подшипник 7 Вибрация Осевая warning_min",
    "SM_Exgauster[2:6]": "Подшипник 7 Вибрация Горизонтальная vibration_horizontal",
    "SM_Exgauster[2:143]": "Подшипник 7 Вибрация Горизонтальная alarm_max",
    "SM_Exgauster[2:155]": "Подшипник 7 Вибрация Горизонтальная alarm_min",
    "SM_Exgauster[2:167]": "Подшипник 7 Вибрация Горизонтальная warning_max",
    "SM_Exgauster[2:179]": "Подшипник 7 Вибрация Горизонтальная warning_min",
    "SM_Exgauster[2:7]": "Подшипник 7 Вибрация Вертикальная vibration_vertical",
    "SM_Exgauster[2:144]": "Подшипник 7 Вибрация Вертикальная alarm_max",
    "SM_Exgauster[2:156]": "Подшипник 7 Вибрация Вертикальная alarm_min",
    "SM_Exgauster[2:168]": "Подшипник 7 Вибрация Вертикальная warning_max",
    "SM_Exgauster[2:180]": "Подшипник 7 Вибрация Вертикальная warning_min",
    "SM_Exgauster[2:34]": "Подшипник 8 Температура нагрева Температура temperature",
    "SM_Exgauster[2:72]": "Подшипник 8 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:81]": "Подшипник 8 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:90]": "Подшипник 8 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:99]": "Подшипник 8 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:11]": "Подшипник 8 Вибрация Осевая vibration_axial",
    "SM_Exgauster[2:148]": "Подшипник 8 Вибрация Осевая alarm_max",
    "SM_Exgauster[2:160]": "Подшипник 8 Вибрация Осевая alarm_min",
    "SM_Exgauster[2:172]": "Подшипник 8 Вибрация Осевая warning_max",
    "SM_Exgauster[2:184]": "Подшипник 8 Вибрация Осевая warning_min",
    "SM_Exgauster[2:9]": "Подшипник 8 Вибрация Горизонтальная vibration_horizontal",
    "SM_Exgauster[2:146]": "Подшипник 8 Вибрация Горизонтальная alarm_max",
    "SM_Exgauster[2:158]": "Подшипник 8 Вибрация Горизонтальная alarm_min",
    "SM_Exgauster[2:170]": "Подшипник 8 Вибрация Горизонтальная warning_max",
    "SM_Exgauster[2:182]": "Подшипник 8 Вибрация Горизонтальная warning_min",
    "SM_Exgauster[2:10]": "Подшипник 8 Вибрация Вертикальная vibration_vertical",
    "SM_Exgauster[2:147]": "Подшипник 8 Вибрация Вертикальная alarm_max",
    "SM_Exgauster[2:159]": "Подшипник 8 Вибрация Вертикальная alarm_min",
    "SM_Exgauster[2:171]": "Подшипник 8 Вибрация Вертикальная warning_max",
    "SM_Exgauster[2:183]": "Подшипник 8 Вибрация Вертикальная warning_min",
    "SM_Exgauster[2:35]": "Подшипник 9 Температура нагрева Температура temperature",
    "SM_Exgauster[2:73]": "Подшипник 9 Температура нагрева Температура alarm_max",
    "SM_Exgauster[2:82]": "Подшипник 9 Температура нагрева Температура alarm_min",
    "SM_Exgauster[2:91]": "Подшипник 9 Температура нагрева Температура warning_max",
    "SM_Exgauster[2:100]": "Подшипник 9 Температура нагрева Температура warning_min",
    "SM_Exgauster[2:42]": "Охладитель Масло  temperature_after",
    "SM_Exgauster[2:41]": "Охладитель Масло  temperature_before",
    "SM_Exgauster[2:37]": "Охладитель Вода  temperature_after",
    "SM_Exgauster[2:36]": "Охладитель Вода temperature_before",
    "SM_Exgauster[2:24]": "Газовый коллектор   temperature_before",
    "SM_Exgauster[2:61]": "Газовый коллектор   underpressure_before",
    "SM_Exgauster[4.1]": "Положение задвижки   gas_valve_closed",
    "SM_Exgauster[4.2]": "Положение задвижки   gas_valve_open",
    "SM_Exgauster[4:6]": "Положение задвижки   gas_valve_position",
    "SM_Exgauster[4:2]": "Главный привод   rotor_current",
    "SM_Exgauster[4:4]": "Главный привод   rotor_voltage",
    "SM_Exgauster[4:3]": "Главный привод   stator_current",
    "SM_Exgauster[4:5]": "Главный привод   stator_voltage",
    "SM_Exgauster[4:0]": "Маслосистема   oil_level",
    "SM_Exgauster[4:1]": "Маслосистема   oil_pressure",
    "SM_Exgauster[2.0]": "Работа эксгаустера   work",
}


def my_assign(consumer, partitions):
    for p in partitions:
        p.offset = Record.objects.last().offset + 1
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
    if not ex.config:
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
    resp = {}
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
        if "SM" in key and key not in keys_not_to_send:
            resp[key] = val

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
                if key in statuses:
                    alarm_max = data[statuses[key]["alarm_max"]]
                    alarm_min = data[statuses[key]["alarm_min"]]
                    warning_max = data[statuses[key]["warning_max"]]
                    warning_min = data[statuses[key]["warning_min"]]
                    if val > alarm_max or val < alarm_min:
                        resp[f"{key}_status"] = "alarm"
                    elif val > warning_max or val < warning_min:
                        resp[f"{key}_status"] = "warning"
                    else:
                        resp[f"{key}_status"] = "normal"
                for amount, approx in approximation.items():
                    vals = values[key][60 - amount :]
                    r = sum(vals) / len(vals)
                    approximation_values[amount][key] = r
                    ExgausterRecordApproximationSignal.objects.create(
                        record=approx, signal=signal, value=r
                    )
            except KeyError:
                continue

    del values["moment"][0]
    values["moment"].append(date)

    # call ml
    # df = pandas.DataFrame.from_dict(values, orient="index").transpose()
    # df.columns = ["moment"] + [
    #     "".join([x for x in el if x != "\\"]) for el in df.columns if el != "moment"
    # ]
    # df = df.rename(columns=mapping)
    # df["moment"] = pandas.to_datetime(df["moment"], utc=True)
    # df = df.set_index("moment").resample("5T").first().reset_index()
    # run(df)

    loop = asyncio.get_event_loop()
    coroutine = send_to_channel_layer(resp)
    loop.run_until_complete(coroutine)

    for approx, data in approximation_values.items():
        loop = asyncio.get_event_loop()
        coroutine = send_to_channel_layer_approximation(data, approx)
        loop.run_until_complete(coroutine)

    print(f"DEBUG: done {offset}, {time.time() - start} sec taken")

c.close()
