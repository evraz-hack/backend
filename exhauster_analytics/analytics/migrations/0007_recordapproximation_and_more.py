# Generated by Django 4.1.7 on 2023-02-17 15:05

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("analytics", "0006_alter_exgaustersignal_characteristics_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="RecordApproximation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "amount",
                    models.IntegerField(
                        choices=[
                            (10, "Ten Minutes"),
                            (30, "Thirty Minutes"),
                            (60, "Hour"),
                        ]
                    ),
                ),
                ("timestamp", models.DateTimeField()),
                ("offset", models.IntegerField()),
            ],
            options={
                "unique_together": {("amount", "offset")},
            },
        ),
        migrations.CreateModel(
            name="ExgausterRecordApproximationSignal",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("value", models.FloatField()),
                (
                    "record",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="signals_approximate",
                        to="analytics.record",
                    ),
                ),
                (
                    "signal",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="records_approximate",
                        to="analytics.exgaustersignal",
                    ),
                ),
            ],
        ),
    ]
