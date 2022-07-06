import inject

from typing import List, Dict
from sdc_client import client
from sdc_client.interfaces import IPipeline, IStreamSetsProvider, IPipelineProvider, ILogger, IStreamSets


class StreamsetsBalancer:
    pipeline_provider = inject.attr(IPipelineProvider)
    logger = inject.attr(ILogger)

    def __init__(self):
        self.streamsets_pipelines: Dict[IStreamSets, List[IPipeline]] = get_streamsets_pipelines()

    def balance(self, asynchronous: bool = False):
        if not asynchronous:
            self._perform_rebalance()
        else:
            self._perform_rebalance_async()

    def _perform_rebalance(self):
        while not self.is_balanced(self.streamsets_pipelines):
            pipeline = self.streamsets_pipelines[most_loaded_streamsets(self.streamsets_pipelines)].pop()
            to_streamsets = least_loaded_streamsets(self.streamsets_pipelines)
            self._move(pipeline, to_streamsets)
            self.logger.info(f'Moved `{pipeline.get_id()}` to `{pipeline.get_streamsets().get_url()}`')

    def _perform_rebalance_async(self):
        # prepare rebalance_map
        streamsets_pipelines = self.streamsets_pipelines.copy()
        rebalance_map = {}
        while not self.is_balanced(streamsets_pipelines):
            pipeline = streamsets_pipelines[most_loaded_streamsets(streamsets_pipelines)].pop()
            to_streamsets = least_loaded_streamsets(streamsets_pipelines)
            streamsets_pipelines[to_streamsets].append(pipeline)
            rebalance_map[pipeline] = to_streamsets

        # actual move
        client.move_to_streamsets_async(rebalance_map)

        # save pipeline with new streamsets
        for pipeline in rebalance_map:
            self.pipeline_provider.save(pipeline)
        self.streamsets_pipelines = streamsets_pipelines.copy()

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
        self.streamsets_pipelines[to_streamsets].append(pipeline)

    @staticmethod
    def is_balanced(streamsets_pipelines: dict) -> bool:
        if len(streamsets_pipelines) < 2:
            return True
        # streamsets are balanced if the difference in num of their pipelines is 0 or 1
        lengths = [len(pipelines) for pipelines in streamsets_pipelines.values()]
        return max(lengths) - min(lengths) < 2


def get_streamsets_pipelines() -> Dict[IStreamSets, List[IPipeline]]:
    pipelines = inject.instance(IPipelineProvider).get_all()
    sp = {}
    for pipeline_ in pipelines:
        streamsets = pipeline_.get_streamsets()
        if streamsets:
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
