from rest_framework import serializers

from exhauster_analytics.analytics.models import Exgauster


class ExgausterSerializer(serializers.ModelSerializer):
    signals = serializers.SerializerMethodField()

    def get_signals(self, obj: Exgauster) -> dict:
        res = {}
        for signal in obj.signals.all():
            data = {
                "name": signal.name,
                "type": signal.type,
                "description": signal.comment,
            }
            if signal.item not in res:
                res[signal.item] = {}
            if (
                signal.characteristics
                and signal.characteristics not in res[signal.item]
            ):
                res[signal.item][signal.characteristics] = {}
            if (
                signal.characteristics_description
                and signal.characteristics_description
                not in res[signal.item][signal.characteristics]
            ):
                res[signal.item][signal.characteristics][
                    signal.characteristics_description
                ] = {}

            if signal.characteristics_description:
                res[signal.item][signal.characteristics][
                    signal.characteristics_description
                ][signal.item_name] = data
            elif signal.characteristics:
                res[signal.item][signal.characteristics][signal.item_name] = data
            else:
                res[signal.item][signal.item_name] = data
        return res

    class Meta:
        model = Exgauster
        fields = ["number", "name", "signals"]


class ExgausterSignalSerializer(serializers.Serializer):
    approximation = serializers.ChoiceField(choices=[1, 10, 30, 60], required=False)
    time_from = serializers.DateTimeField(required=False)
    time_until = serializers.DateTimeField(required=False)
    signals = serializers.ListSerializer(child=serializers.CharField(max_length=30))
