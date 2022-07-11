import requests
import aiohttp
from random import randint

from unittest.mock import Mock, MagicMock
from sdc_client import IPipelineProvider


class PipelineMock:
    @staticmethod
    def get_id():
        return 'id'

    @staticmethod
    def set_streamsets(s):
        pass

    @staticmethod
    def get_streamsets():
        o = Mock()
        o.get_url = MagicMock(return_value='url')
        return o


class StreamSetsMock:
    def __init__(self):
        self.id = randint(0, 1000)

    @staticmethod
    def get_url():
        return 'url'

    @staticmethod
    def get_username():
        return 'admin'

    @staticmethod
    def get_password():
        return 'admin'

    # @staticmethod
    def get_id(self):
        return self.id


class MockAsyncResponse:
    def __init__(self, _text, status_code):
        self._text = _text
        self.status = status_code

    async def text(self):
        return self._text

    async def json(self):
        return {'json': self._text}

    def raise_for_status(self):
        if self.status == 200:
            return None
        raise aiohttp.ClientResponseError(
            status=self.status,
            history=None,
            request_info=None,
        )

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


class MockResponse:
    def __init__(self, _text, status_code):
        self._text = _text
        self.status_code = status_code

    @property
    def text(self):
        return self._text

    def json(self):
        return {'json': self._text}

    def raise_for_status(self):
        if self.status_code == 200:
            return None
        response = requests.Response()
        response.status_code = self.status_code
        response.raise_for_status()


def instance(type_: type):
    if type_ == IPipelineProvider:
        pipeline = PipelineMock()
        pipeline.get_streamsets = MagicMock(return_value=StreamSetsMock())
        res = [pipeline]
    else:
        streamsets = StreamSetsMock()
        res = [streamsets]
    mock = Mock()
    mock.get_all = MagicMock(return_value=res)
    return mock
