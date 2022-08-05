import inject
import itertools

from typing import List, Dict
from sdc_client import client
from sdc_client.interfaces import IPipeline, IStreamSetsProvider, IPipelineProvider, ILogger, IStreamSets


class StreamsetsBalancer:
    pipeline_provider = inject.attr(IPipelineProvider)
    logger = inject.attr(ILogger)

    def __init__(self):
        self.streamsets_pipelines: Dict[IStreamSets, List[IPipeline]] = get_streamsets_pipelines()
        self._preferred_types = set([ss.get_preferred_type() for ss in self.streamsets_pipelines if ss.get_preferred_type()])
        self._pipelines = list(itertools.chain.from_iterable(self.streamsets_pipelines.values()))
        self._streamsets = list(self.streamsets_pipelines.keys())
        self.balanced_streamsets_pipelines: Dict[IStreamSets, List[IPipeline]] = {ss: [] for ss in self._streamsets}
        self.rebalance_map = {}
        self.structure = {}
        for type_ in self._preferred_types:
            self.structure[type_] = {}
            self.structure[type_]['streamsets'] = [ss for ss in self._streamsets if ss.get_preferred_type() == type_]
            self.structure[type_]['pipelines'] = [pipeline for pipeline in self._pipelines if pipeline.type == type_]
        self.structure[None] = {}
        self.structure[None]['streamsets'] = [ss for ss in self._streamsets if not ss.get_preferred_type() in self._preferred_types]
        self.structure[None]['pipelines'] = [pipeline for pipeline in self._pipelines if not pipeline.type in self._preferred_types]

    def _balance_type(self, type_: str):
        # Set all typed pipelines to first streamsets with same type
        pipelines = self.structure[type_]['pipelines']
        streamsets = self.structure[type_]['streamsets']
        for pipeline_ in pipelines:
            self.rebalance_map[pipeline_] = streamsets[0]
            self.balanced_streamsets_pipelines[streamsets[0]].append(pipeline_)
        sub_pipelines_dict = dict()
        sub_pipelines_dict[streamsets[0]] = pipelines
        for ss in streamsets[1:]:
            sub_pipelines_dict[ss] = []
        # balance it
        self._balance(sub_pipelines_dict)

    def _balance(self, streamsets_pipelines: Dict[IStreamSets, List[IPipeline]]):
        while not self.is_balanced(streamsets_pipelines):
            streamsets = most_loaded_streamsets(streamsets_pipelines)
            pipeline = self.streamsets_pipelines[streamsets].pop()
            to_streamsets = least_loaded_streamsets(streamsets_pipelines)
            streamsets_pipelines[to_streamsets].append(pipeline)
            self.rebalance_map[pipeline] = to_streamsets
            self.balanced_streamsets_pipelines[to_streamsets].append(pipeline)

    def balance(self):
        for type_ in self._preferred_types:
            self._balance_type(type_)

        for pipeline in self.structure[None]['pipelines']:
            streamsets = least_loaded_streamsets(self.balanced_streamsets_pipelines)
            self.rebalance_map[pipeline] = streamsets
            self.balanced_streamsets_pipelines[streamsets].append(pipeline)
        self._apply_rebalance_map()
        self.streamsets_pipelines = self.balanced_streamsets_pipelines.copy()
        return self.rebalance_map

    def _apply_rebalance_map(self):
        for pipeline, to_streamsets in self.rebalance_map.items():
            self._move(pipeline, self.rebalance_map[pipeline])

    def unload_streamsets(self, streamsets: IStreamSets):
        for pipeline in self.streamsets_pipelines.pop(streamsets):
            to_streamsets = least_loaded_streamsets(self.streamsets_pipelines)
            self._move(pipeline, to_streamsets)

    def _move(self, pipeline: IPipeline, to_streamsets: IStreamSets):
        self.logger.info(
            f'Moving `{pipeline.get_id()}` from `{pipeline.get_streamsets().get_url()}` to `{to_streamsets.get_url()}`'
        )
        should_start = client.get_pipeline_status(pipeline) in [IPipeline.STATUS_STARTING, IPipeline.STATUS_RUNNING]
        client.delete(pipeline)
        pipeline.set_streamsets(to_streamsets)
        client.create(pipeline)
        self.pipeline_provider.save(pipeline)
        if should_start:
            client.start(pipeline)
        # self.streamsets_pipelines[to_streamsets].append(pipeline)
        self.logger.info(f'Moved `{pipeline.get_id()}` to `{pipeline.get_streamsets().get_url()}`')

    @staticmethod
    def is_balanced(streamsets_pipelines: dict) -> bool:
        if len(streamsets_pipelines) < 2:
            return True
        # streamsets are balanced if the difference in num of their pipelines is 0 or 1
        lengths = [len(pipelines) for pipelines in streamsets_pipelines.values()]
        if max(lengths) - min(lengths) < 2:
            return True
        most_loaded_streamsets_ = most_loaded_streamsets(streamsets_pipelines)
        for pipeline in streamsets_pipelines[most_loaded_streamsets_]:
            if pipeline.type != most_loaded_streamsets_.get_preferred_type():
                return False
        return StreamsetsBalancer.is_balanced(
            {k: v for k, v in streamsets_pipelines.items() if k != most_loaded_streamsets_}
        )


def get_streamsets_pipelines() -> Dict[IStreamSets, List[IPipeline]]:
    pipelines = inject.instance(IPipelineProvider).get_all()
    sp = {}
    for pipeline_ in pipelines:
        if streamsets := pipeline_.get_streamsets():
            if streamsets not in sp:
                sp[streamsets] = []
            sp[streamsets].append(pipeline_)
    for streamsets_ in inject.instance(IStreamSetsProvider).get_all():
        if streamsets_ not in sp:
            sp[streamsets_] = []
    return sp


def most_loaded_streamsets(streamsets_pipelines: Dict[IStreamSets, List[IPipeline]]) -> IStreamSets:
    return max(streamsets_pipelines, key=lambda x: len(streamsets_pipelines[x]))


def least_loaded_streamsets(streamsets_pipelines: Dict[IStreamSets, List[IPipeline]]) -> IStreamSets:
    return min(streamsets_pipelines, key=lambda x: len(streamsets_pipelines[x]))
