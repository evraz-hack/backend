from django.db import models


class Record(models.Model):
    received = models.DateTimeField(auto_now_add=True)
    timestamp = models.DateTimeField(blank=False)

    offset = models.IntegerField(unique=True)

    # store message for further usage, e. g. reading logs, etc
    message = models.JSONField(null=True)

    class Meta:
        ordering = ["offset"]


class RecordApproximation(models.Model):
    class AmountChoices(models.IntegerChoices):
        ten_minutes = 10
        thirty_minutes = 30
        hour = 60

    amount = models.IntegerField(choices=AmountChoices.choices)
    timestamp = models.DateTimeField(blank=False)
    offset = models.IntegerField()

    class Meta:
        unique_together = ["amount", "offset"]


class ExgausterRecordSignal(models.Model):
    record = models.ForeignKey(
        "analytics.Record", related_name="signals", on_delete=models.CASCADE
    )
    signal = models.ForeignKey(
        "analytics.ExgausterSignal", related_name="records", on_delete=models.CASCADE
    )
    value = models.FloatField()


class ExgausterRecordApproximationSignal(models.Model):
    record = models.ForeignKey(
        "analytics.RecordApproximation", related_name="signals", on_delete=models.CASCADE
    )
    signal = models.ForeignKey(
        "analytics.ExgausterSignal",
        related_name="records_approximate",
        on_delete=models.CASCADE,
    )
    value = models.FloatField()


class Exgauster(models.Model):
    number = models.IntegerField(unique=True)
    name = models.CharField(max_length=20)


class ExgausterSignal(models.Model):
    class ExgausterSignalType(models.TextChoices):
        ANALOG = "analog", "ANALOG"
        DIGITAL = "digital", "DIGITAL"

    exgauster = models.ForeignKey(
        "analytics.Exgauster", related_name="signals", on_delete=models.CASCADE
    )

    # store place in x:y way
    place_x = models.IntegerField()
    place_y = models.IntegerField()

    type = models.CharField(
        choices=ExgausterSignalType.choices,
        max_length=7,
    )
    comment = models.CharField(max_length=200, blank=True)
    active = models.BooleanField(default=True)

    # Подшипник1 - Температура нагрева - Температура - temperature
    item = models.CharField(max_length=200, blank=True)
    characteristics = models.CharField(max_length=200, blank=True)
    characteristics_description = models.CharField(max_length=200, blank=True)
    item_name = models.CharField(max_length=200, blank=True)

    @property
    def name(self) -> str:
        return f"SM_Exgauster\\[{self.place_x}{':' if self.type == 'analog' else '.'}{self.place_y}]"

    class Meta:
        unique_together = ["place_x", "place_y", "type"]
