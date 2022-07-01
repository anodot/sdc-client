import unittest

from unittest.mock import MagicMock, Mock
from sdc_client import StreamsetsBalancer, client, balancer
from conftest import StreamSetsMock, PipelineMock


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


if __name__ == '__main__':
    unittest.main()
