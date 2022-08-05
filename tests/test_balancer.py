import unittest
import inject
import json

from sdc_client import balancer
from conftest import StreamSetsMock, PipelineMock, instance


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

    def test_typed_streamsets_is_balanced_1(self):
        s1 = StreamSetsMock(type_='dir')
        s2 = StreamSetsMock(type_='not_dir')
        data = {
            s1: [PipelineMock(type_='dir'),
                 PipelineMock(type_='dir'),
                 ],
            s2: [],
        }
        assert balancer.StreamsetsBalancer.is_balanced(data)

    def test_specific_streamsets(self):
        s1 = StreamSetsMock(type_='dir')
        s2 = StreamSetsMock()
        s3 = StreamSetsMock(type_='not_dir')
        data = {
            s1: [PipelineMock(type_='dir'),
                 PipelineMock(type_='dir'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='1'),
                 PipelineMock(type_='not_dir'),
                 ],
            s2: [
                PipelineMock(type_='1'),
            ],
            s3: [
                PipelineMock(type_='dir'),
                PipelineMock(type_='1'),
                PipelineMock(type_='1'),
                PipelineMock(type_='not_dir'),
            ],
        }

        def _move(self, pipeline_, to_streamsets):
            for ss in self.streamsets_pipelines:
                if pipeline_ in self.streamsets_pipelines[ss]:
                    self.streamsets_pipelines[ss].remove(pipeline_)
            self.streamsets_pipelines[to_streamsets].append(pipeline_)

        balancer.get_streamsets_pipelines = lambda: data.copy()
        balancer.StreamsetsBalancer._move = _move
        balancer_ = balancer.StreamsetsBalancer()
        balancer_.balance()

        assert balancer_.is_balanced(balancer_.streamsets_pipelines)

        balancer_2 = balancer.StreamsetsBalancer()
        balancer_2.rebalance_map = balancer_.rebalance_map
        balancer_2._apply_rebalance_map()
        assert balancer_.is_balanced(balancer_2.streamsets_pipelines)

if __name__ == '__main__':
    unittest.main()
