import unittest
import requests
import aiohttp

from unittest.mock import MagicMock, AsyncMock
from sdc_client import client
from sdc_client.base_api_client import UnauthorizedException
from conftest import MockResponse, MockAsyncResponse, StreamSetsMock


class TestGetJMX(unittest.TestCase):
    def test_get_jmx_success(self):
        requests.Session.get = MagicMock(return_value=MockResponse(
            _text='data',
            status_code=200
        ))
        result = client.get_jmx(StreamSetsMock(), 'query_params_1')
        self.assertEqual(result, {'json': 'data'})

    def test_get_jmx_unauthorized(self):
        requests.Session.get = MagicMock(return_value=MockResponse(
            _text='data',
            status_code=401,
        ))
        try:
            result = client.get_jmx(StreamSetsMock(), 'query_params_1')
        except Exception as e:
            self.assertIsInstance(e, UnauthorizedException)
        else:
            raise Exception("Code must raise error for this case")

    def test_get_jmx_async_success(self):
        aiohttp.ClientSession.get = AsyncMock(return_value=MockAsyncResponse(
            _text='data',
            status_code=200
        ))
        queries = [
            (StreamSetsMock(), 'query_params_1',),
            (StreamSetsMock(), 'query_params_2',),
        ]
        result = client.get_jmxes_async(queries=queries)
        self.assertEqual(result, [{'json': 'data'}, {'json': 'data'}])

    def test_get_jmx_async_unauthorized(self):
        aiohttp.ClientSession.get = AsyncMock(return_value=MockAsyncResponse(
            _text='data',
            status_code=401
        ))
        queries = [
            (StreamSetsMock(), 'query_params_1',),
            (StreamSetsMock(), 'query_params_2',),
        ]
        try:
            result = client.get_jmxes_async(queries=queries)
        except Exception as e:
            self.assertIsInstance(e, UnauthorizedException)
