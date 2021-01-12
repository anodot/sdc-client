import unittest
import inject

from unittest.mock import MagicMock, Mock
from sdc_client import StreamsetsBalancer, balancer, client, IPipelineProvider


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
    @staticmethod
    def get_url():
        return 'url'


class TestStreamSetsBalancer(unittest.TestCase):
    def setUp(self):
        client.get_pipeline_status = MagicMock(return_value='bla')
        client.delete = Mock()
        client.create = Mock()
        balancer.get_streamsets_pipelines = MagicMock(return_value={1: [Mock()]})
        self.balancer = StreamsetsBalancer()
        self.balancer.logger = Mock()
        self.balancer.logger.info = Mock()
        self.balancer.pipeline_provider = Mock()
        self.balancer.pipeline_provider.save = Mock()

    def test_balanced(self):
        assert self.balancer.is_balanced()

    def test_balanced_2(self):
        self.balancer.streamsets_pipelines = {1: [Mock()], 2: [Mock()], 3: [Mock()]}
        assert self.balancer.is_balanced()

    def test_not_balanced(self):
        self.balancer.streamsets_pipelines = {1: [Mock(), Mock()], 2: []}
        assert not self.balancer.is_balanced()

    def test_not_balanced_2(self):
        self.balancer.streamsets_pipelines = {
            1: [Mock(), Mock()],
            2: [Mock(), Mock()],
            3: [],
        }
        assert not self.balancer.is_balanced()

    def test_balance(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        s3 = StreamSetsMock()
        s4 = StreamSetsMock()
        self.balancer.streamsets_pipelines = {
            s1: [PipelineMock(), PipelineMock(), PipelineMock()],
            s2: [],
            s3: [PipelineMock()],
            s4: [],
        }
        self.balancer.balance()
        assert len(self.balancer.streamsets_pipelines[s1]) == 1
        assert len(self.balancer.streamsets_pipelines[s2]) == 1
        assert len(self.balancer.streamsets_pipelines[s3]) == 1
        assert len(self.balancer.streamsets_pipelines[s4]) == 1

    def test_balance_2(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        s3 = StreamSetsMock()
        self.balancer.streamsets_pipelines = {
            s1: [PipelineMock(), PipelineMock(), PipelineMock(), PipelineMock()],
            s2: [],
            s3: [],
        }
        self.balancer.balance()
        assert len(self.balancer.streamsets_pipelines[s1]) == 2
        assert len(self.balancer.streamsets_pipelines[s2]) == 1
        assert len(self.balancer.streamsets_pipelines[s3]) == 1


def instance(type_: type):
    if type_ == IPipelineProvider:
        pipeline = PipelineMock()
        pipeline.get_streamsets = MagicMock(return_value=StreamSetsMock())
        res = [pipeline]
    else:
        streamsets = StreamSetsMock()
        res = [streamsets]
    m = Mock()
    m.get_all = MagicMock(return_value=res)
    return m


class TestBalancer(unittest.TestCase):
    def setUp(self):
        inject.instance = instance

    def test_get_streamsets_pipelines(self):
        sp = balancer.get_streamsets_pipelines()
        assert len(sp.keys()) == 2

    def test_most_loaded_streamsets(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        r = balancer.most_loaded_streamsets({
            s1: [PipelineMock()],
            s2: []
        })
        assert r == s1

    def test_most_loaded_streamsets_2(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        s3 = StreamSetsMock()
        r = balancer.most_loaded_streamsets({
            s1: [PipelineMock(), PipelineMock()],
            s2: [],
            s3: [PipelineMock(), PipelineMock(), PipelineMock()],
        })
        assert r == s3

    def test_least_loaded_streamsets(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        r = balancer.least_loaded_streamsets({
            s1: [PipelineMock()],
            s2: []
        })
        assert r == s2

    def test_least_loaded_streamsets_2(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        s3 = StreamSetsMock()
        r = balancer.least_loaded_streamsets({
            s1: [PipelineMock(), PipelineMock()],
            s2: [],
            s3: [PipelineMock(), PipelineMock(), PipelineMock()],
        })
        assert r == s2

    def test_least_loaded_streamsets_exclude(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        s3 = StreamSetsMock()
        r = balancer.least_loaded_streamsets({
            s1: [PipelineMock(), PipelineMock()],
            s2: [],
            s3: [PipelineMock(), PipelineMock(), PipelineMock()],
        }, exclude=s2)
        assert r == s1
