from rest_framework import generics
from rest_framework.response import Response

from exhauster_analytics.analytics.services import get_signal_values
from exhauster_analytics.analytics.api.serializers import (
    ExgausterSerializer,
    ExgausterSignalSerializer,
)
from exhauster_analytics.analytics.models import Exgauster


class ListExgauster(generics.ListAPIView):
    serializer_class = ExgausterSerializer
    queryset = Exgauster.objects.all()
    permission_classes = []


class GetApproximatedExgausterSignals(generics.GenericAPIView):
    serializer_class = ExgausterSignalSerializer
    permission_classes = []

    def post(self, request, *args, **kwargs):
        serializer = ExgausterSignalSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response(data=get_signal_values(**serializer.data))
