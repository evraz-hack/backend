import pandas
from rest_framework.exceptions import NotFound

from exhauster_analytics.analytics.models import (
    ExgausterSignal,
    Record,
    RecordApproximation,
    ExgausterRecordSignal, ExgausterRecordApproximationSignal,
)


def get_signal_values(signals, approximation=None, time_from=None, time_until=None):
    exhauster_signals = []
    signals_by_id = {}
    for signal in signals:
        try:
            if "." in signals:
                x, y = map(
                    int, signal[signal.find("[") + 1 : signal.find("]")].split(".")
                )
                type = "digital"
            else:
                x, y = map(
                    int, signal[signal.find("[") + 1 : signal.find("]")].split(":")
                )
                type = "analog"
            signal_obj = ExgausterSignal.objects.get(place_x=x, place_y=y, type=type)
            signals_by_id[signal_obj.id] = signal
        except (ExgausterSignal.DoesNotExist, ValueError):
            raise NotFound("signal not found")
        exhauster_signals.append(signal_obj)

    if not approximation or approximation == 1:
        if time_from:
            if time_until:
                records = Record.objects.filter(
                    timestamp__gte=time_from, timestamp__lte=time_until
                )
            else:
                records = Record.objects.filter(timestamp__gte=time_from)
        else:
            if time_until:
                records = Record.objects.filter(timestamp__lte=time_until)
            else:
                records = Record.objects.all()
        vals = ExgausterRecordSignal.objects.filter(
            record__in=records, signal__in=exhauster_signals
        ).values_list("signal", "value")
    else:
        if time_from:
            if time_until:
                records = RecordApproximation.objects.filter(
                    timestamp__gte=time_from,
                    timestamp__lte=time_until,
                    amount=approximation,
                )
            else:
                records = RecordApproximation.objects.filter(
                    timestamp__gte=time_from, amount=approximation
                )
        else:
            if time_until:
                records = RecordApproximation.objects.filter(
                    timestamp__lte=time_until, amount=approximation
                )
            else:
                records = RecordApproximation.objects.filter(amount=approximation)
        vals = ExgausterRecordApproximationSignal.objects.filter(
            record__in=records, signal__in=exhauster_signals
        ).values_list("signal_id", "value")
    d = {}
    for x, y in vals:
        d.setdefault(signals_by_id[x], []).append(y)
    return d


# function to parse signal mapping from xlsx file
def f(ex, num):
    df = pandas.read_excel(
        "Маппинг сигналов.xlsx", sheet_name=num, nrows=105, index_col=[4]
    )
    df = df.fillna(method="ffill")
    for index, row in df.iterrows():
        row = row.values.tolist()
        s = index
        if "." in s:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split("."))
        else:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split(":"))
        ExgausterSignal.objects.create(
            exgauster=ex,
            place_x=x,
            place_y=y,
            type=row[5],
            comment=row[4],
            active=bool(row[6]),
            item=row[0],
            characteristics=row[1],
            characteristics_description=row[2],
            item_name=row[3],
        )

    df = pandas.read_excel(
        "Маппинг сигналов.xlsx", sheet_name=num, skiprows=105, nrows=4, index_col=[4]
    )
    df = df.fillna(method="ffill")
    for index, row in df.iterrows():
        row = row.values.tolist()
        s = index
        if "." in s:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split("."))
        else:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split(":"))
        ExgausterSignal.objects.create(
            exgauster=ex,
            place_x=x,
            place_y=y,
            type=row[5],
            comment=row[4],
            active=bool(row[6]),
            item=row[0],
            characteristics=row[1],
            characteristics_description=row[2],
            item_name=row[3],
        )

    df = pandas.read_excel(
        "Маппинг сигналов.xlsx", sheet_name=num, skiprows=109, index_col=[4]
    )
    df = df.fillna(method="ffill")
    for index, row in df.iterrows():
        row = row.values.tolist()
        s = index
        if "." in s:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split("."))
        else:
            x, y = map(int, s[s.find("[") + 1 : s.find("]")].split(":"))
        ExgausterSignal.objects.create(
            exgauster=ex,
            place_x=x,
            place_y=y,
            type=row[5],
            comment=row[4],
            active=bool(row[6]),
            item=row[0],
            item_name=row[3],
        )
