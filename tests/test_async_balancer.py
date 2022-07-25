import unittest
import inject

from sdc_client import async_balancer, balancer
from conftest import StreamSetsMock, PipelineMock, instance
from unittest.mock import patch


class TestBalancerAsync(unittest.TestCase):
    def setUp(self):
        inject.instance = instance

    def test_x_loaded_streamsets(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        value = {
            s1: [PipelineMock(), PipelineMock()],
            s2: []
        }
        with patch.object(balancer, 'get_streamsets_pipelines') as m:
            m.return_value = value

            b = async_balancer.StreamsetsBalancerAsync()
            s1_expected = async_balancer.most_loaded_streamsets(b.streamsets_pipelines)
            s2_expected = async_balancer.least_loaded_streamsets(b.streamsets_pipelines)
            assert s1_expected == s1
            assert s2_expected == s2

    def test_balance_not_needed(self):
        s1 = StreamSetsMock()
        s2 = StreamSetsMock()
        value = {
            s1: [PipelineMock(), PipelineMock()],
            s2: [PipelineMock()]
        }
        with patch.object(balancer, 'get_streamsets_pipelines') as m:
            m.return_value = value

            b = async_balancer.StreamsetsBalancerAsync()
            sp_before = b.streamsets_pipelines
            b.balance()
            assert sp_before == b.streamsets_pipelines


if __name__ == '__main__':
    unittest.main()
