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
            async_balancer.StreamsetsBalancerAsync._apply_rebalance_map = lambda x: print('_apply_rebalance_map')
            b = async_balancer.StreamsetsBalancerAsync()
            sp_before = b.streamsets_pipelines
            b.balance()
            assert b.is_balanced(b.balanced_streamsets_pipelines)

    def test_specific_streamsets(self):
        s1 = StreamSetsMock(type_='dir')
        s2 = StreamSetsMock(type_='dir')
        s3 = StreamSetsMock(type_='not_dir')
        data = {
            s1: [PipelineMock(type_='dir'),
                 PipelineMock(type_='dir'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='not_dir'),
            ],
            s2: [],
            s3: [
                PipelineMock(type_='dir'),
                PipelineMock(type_='2'),
                PipelineMock(type_='2'),
                PipelineMock(type_='2'),
                PipelineMock(type_='not_dir'),
            ],
        }

        balancer.get_streamsets_pipelines = lambda: data.copy()
        balancer_ = async_balancer.StreamsetsBalancerAsync()
        balancer_.balance()
        assert not balancer_.is_balanced(data)
        assert balancer_.is_balanced(balancer_.balanced_streamsets_pipelines)


if __name__ == '__main__':
    unittest.main()
