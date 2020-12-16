import inject

from sdc_client.client import Client, choose_streamsets
from sdc_client.interfaces import IPipeline, IStreamSetsProvider, IPipelineProvider, ILogger, IStreamSets


class StreamsetsBalancer:
    streamsets_provider = inject.attr(IStreamSetsProvider)
    pipeline_provider = inject.attr(IPipelineProvider)
    logger = inject.attr(ILogger)

    def __init__(self):
        self.streamsets_pipelines: dict[int, list[IPipeline]] = self._get_streamsets_pipelines()
        self.client = Client()

    def balance(self):
        while not self.is_balanced():
            pipeline = self.streamsets_pipelines[self._get_busiest_streamsets_id()].pop()
            to_streamsets = choose_streamsets(
                self.pipeline_provider.count_by_streamsets(),
                self.streamsets_provider.get_all()
            )
            self._move(pipeline, to_streamsets)
            self.streamsets_pipelines[pipeline.get_streamsets().get_id()].append(pipeline)
            self.logger.info(f'Moved `{pipeline.get_id()}` to `{pipeline.get_streamsets().get_url()}`')

    def unload_streamsets(self, streamsets: IStreamSets):
        for pipeline in self.streamsets_pipelines[streamsets.get_id()]:
            to_streamsets = choose_streamsets(
                self.pipeline_provider.count_by_streamsets(),
                self.streamsets_provider.get_all(),
                exclude=streamsets.get_id()
            )
            self._move(pipeline, to_streamsets)

    def _move(self, pipeline: IPipeline, to_streamsets: IStreamSets):
        self.logger.info(
            f'Moving `{pipeline.get_id()}` from `{pipeline.get_streamsets().get_url()}` to `{to_streamsets.get_url()}`'
        )
        # todo ниче что зависимость от inject?
        should_start = self.client.get_pipeline_status(pipeline) in [IPipeline.STATUS_STARTING, IPipeline.STATUS_RUNNING]

        self.client.delete(pipeline)
        pipeline.set_streamsets(to_streamsets)
        self.client.create(pipeline)
        self.pipeline_provider.save(pipeline)
        if should_start:
            self.client.start(pipeline)

    def _get_streamsets_pipelines(self) -> dict:
        pipelines = self.pipeline_provider.get_pipelines()
        # todo remove when monitoring is deleted
        pipelines = filter(
            lambda p: not p.get_name.startswith('Monitoring'),
            pipelines
        )
        sp = {}
        for pipeline_ in pipelines:
            s_id = pipeline_.get_streamsets().get_id()
            if s_id not in sp:
                sp[s_id] = []
            sp[s_id].append(pipeline_)
        for streamsets_ in self.streamsets_provider.get_all():
            if streamsets_.get_id() not in sp:
                sp[streamsets_.get_id()] = []
        return sp

    def is_balanced(self) -> bool:
        if len(self.streamsets_pipelines.keys()) < 2:
            return True
        # streamsets are balanced if the difference in num of their pipelines is 0 or 1
        lengths = [len(pipelines) for pipelines in self.streamsets_pipelines.values()]
        return max(lengths) - min(lengths) < 2

    def _get_busiest_streamsets_id(self) -> int:
        key, _ = max(self.streamsets_pipelines.items(), key=lambda x: len(x))
        return key
