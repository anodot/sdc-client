import json
import re
import time
import inject

from enum import Enum
from datetime import datetime
from typing import Optional
from api_client import _StreamSetsApiClient, _ApiClientException, _PipelineFreezeException
from src import ILogger, IStreamSets, IStreamSetsProvider, IPipelineProvider, IPipeline


class Severity(Enum):
    INFO = 'info'
    ERROR = 'error'


class Client:
    streamsets_provider = inject.attr(IStreamSetsProvider)
    pipeline_provider: inject.attr(IPipelineProvider)
    logger: inject.attr(ILogger)

    def __init__(self):
        self.clients: dict[int, _StreamSetsApiClient] = {}

    def _client(self, pipeline: IPipeline) -> _StreamSetsApiClient:
        if not pipeline.get_streamsets():
            raise StreamsetsException(f'Pipeline `{pipeline.get_id()}` does not belong to any StreamSets')
        if pipeline.get_streamsets().get_id() not in self.clients:
            self.clients[pipeline.get_streamsets().get_id()] = _StreamSetsApiClient(pipeline.get_streamsets())
        return self.clients[pipeline.get_streamsets().get_id()]

    def create(self, pipeline: IPipeline):
        if not pipeline.get_streamsets():
            pipeline.set_streamsets(choose_streamsets(
                self.pipeline_provider.count_by_streamsets(),
                self.streamsets_provider.get_all()
            ))
        try:
            self._client(pipeline).create_pipeline(pipeline.get_id())
        except _ApiClientException as e:
            raise StreamsetsException(str(e))
        try:
            self._update_pipeline(pipeline)
        except _ApiClientException as e:
            self.delete(pipeline)
            raise StreamsetsException(str(e))

    def update(self, pipeline: IPipeline):
        start_pipeline = False
        try:
            if self.get_pipeline_status(pipeline) in [IPipeline.STATUS_RUNNING, IPipeline.STATUS_RETRY]:
                self.stop(pipeline)
                start_pipeline = True
            self._update_pipeline(pipeline)
        except _ApiClientException as e:
            raise StreamsetsException(str(e))
        if start_pipeline:
            self.start(pipeline)

    # todo it's used only for tests
    def get_pipeline(self, pipeline: IPipeline) -> dict:
        return self._client(pipeline).get_pipeline(pipeline.get_id())

    def get_pipeline_logs(self, pipeline: IPipeline, severity: Severity, number_of_records: int) -> list:
        client = self._client(pipeline)
        return _transform_logs(
            client.get_pipeline_logs(pipeline.get_id(), severity)[:number_of_records]
        )

    def get_pipeline_status_by_id(self, pipeline: IPipeline) -> str:
        return self.get_pipeline_status(pipeline)

    def get_pipeline_status(self, pipeline: IPipeline) -> str:
        return self._client(pipeline).get_pipeline_status(pipeline.get_id())['status']

    def get_pipeline_metrics(self, pipeline: IPipeline) -> dict:
        return self._client(pipeline).get_pipeline_metrics(pipeline.get_id())

    def get_all_pipelines(self) -> list[dict]:
        pipelines = []
        for streamsets_ in self.streamsets_provider.get_all():
            client = _StreamSetsApiClient(streamsets_)
            pipelines = pipelines + client.get_pipelines()
        return pipelines

    def get_all_pipeline_statuses(self) -> dict:
        statuses = {}
        for streamsets_ in self.streamsets_provider.get_all():
            client = _StreamSetsApiClient(streamsets_)
            statuses = {**statuses, **client.get_pipeline_statuses()}
        return statuses

    def get_pipeline_info(self, pipeline: IPipeline, number_of_history_records: int) -> dict:
        client = self._client(pipeline)
        status = client.get_pipeline_status(pipeline.get_id())
        metrics = json.loads(status['metrics']) if status['metrics'] else self.get_pipeline_metrics(pipeline)
        pipeline_info = client.get_pipeline(pipeline.get_id())
        history = client.get_pipeline_history(pipeline.get_id())
        info = {
            'status': '{status} {message}'.format(**status),
            'metrics': self._get_metrics_string(metrics),
            'metric_errors': self._get_metric_errors(pipeline, metrics),
            'pipeline_issues': self._extract_pipeline_issues(pipeline_info),
            'stage_issues': self._extract_stage_issues(pipeline_info),
            'history': self._get_history_info(history, number_of_history_records)
        }
        return info

    @staticmethod
    def _get_metrics_string(metrics_: dict) -> str:
        if metrics_:
            stats = {
                'in': metrics_['counters']['pipeline.batchInputRecords.counter']['count'],
                'out': metrics_['counters']['pipeline.batchOutputRecords.counter']['count'],
                'errors': metrics_['counters']['pipeline.batchErrorRecords.counter']['count'],
            }
            stats['errors_perc'] = stats['errors'] * 100 / stats['in'] if stats['in'] != 0 else 0
            return 'In: {in} - Out: {out} - Errors {errors} ({errors_perc:.1f}%)'.format(**stats)
        return ''

    def _get_metric_errors(self, pipeline: IPipeline, metrics: Optional[dict]) -> list:
        errors = []
        if metrics:
            for name, counter in metrics['counters'].items():
                stage_name = re.search('stage\.(.+)\.errorRecords\.counter', name)
                if counter['count'] == 0 or not stage_name:
                    continue
                for error in self._client(pipeline).get_pipeline_errors(pipeline.get_id(), stage_name.group(1)):
                    errors.append(
                        f'{_format_timestamp(error["header"]["errorTimestamp"])} - {error["header"]["errorMessage"]}')
        return errors

    @staticmethod
    def _extract_pipeline_issues(pipeline_info: dict) -> list:
        pipeline_issues = []
        if pipeline_info['issues']['issueCount'] > 0:
            for issue in pipeline_info['issues']['pipelineIssues']:
                pipeline_issues.append(_format(issue))
        return pipeline_issues

    @staticmethod
    def _extract_stage_issues(pipeline_info: dict) -> dict:
        stage_issues = {}
        if pipeline_info['issues']['issueCount'] > 0:
            for stage, issues in pipeline_info['issues']['stageIssues'].items():
                if stage not in stage_issues:
                    stage_issues[stage] = []
                for i in issues:
                    stage_issues[stage].append('{severity} - {configGroup} - {configName} - {message}'.format(**i))
        return stage_issues

    def _get_history_info(self, history: list, number_of_history_records: int) -> list:
        info = []
        for item in history[:number_of_history_records]:
            metrics_str = self._get_metrics_string(json.loads(item['metrics'])) if item['metrics'] else ' '
            message = item['message'] if item['message'] else ' '
            info.append([_format_timestamp(item['timeStamp']), item['status'], message, metrics_str])
        return info

    def force_stop(self, pipeline: IPipeline):
        client = self._client(pipeline)
        try:
            client.stop_pipeline(pipeline.get_id())
        except _ApiClientException:
            pass
        if not self.get_pipeline_status_by_id(pipeline) == IPipeline.STATUS_STOPPING:
            raise PipelineException("Can't force stop a pipeline not in the STOPPING state")
        client.force_stop(pipeline.get_id())
        client.wait_for_status(pipeline.get_id(), IPipeline.STATUS_STOPPED)

    def stop(self, pipeline: IPipeline):
        self.logger.info(f'Stopping the pipeline `{pipeline.get_id()}`')
        client = self._client(pipeline)
        client.stop_pipeline(pipeline.get_id())
        try:
            client.wait_for_status(pipeline.get_id(), IPipeline.STATUS_STOPPED)
        except _PipelineFreezeException:
            self.logger.info(f'Force stopping the pipeline `{pipeline.get_id()}`')
            self.force_stop(pipeline)

    def reset(self, pipeline: IPipeline):
        self._client(pipeline).reset_pipeline(pipeline.get_id())

    def delete(self, pipeline: IPipeline):
        if self.get_pipeline_status(pipeline) in [IPipeline.STATUS_RUNNING, IPipeline.STATUS_RETRY]:
            self.stop(pipeline)
        self._client(pipeline).delete_pipeline(pipeline.get_id())
        pipeline.delete_streamsets()

    # todo return type
    def create_preview(self, pipeline: IPipeline):
        return self._client(pipeline).create_preview(pipeline.get_id())

    def wait_for_preview(self, pipeline: IPipeline, preview_id: str):
        return self._client(pipeline).wait_for_preview(pipeline.get_id(), preview_id)

    def start(self, pipeline: IPipeline, wait_for_sending_data: bool = False):
        client = self._client(pipeline)
        client.start_pipeline(pipeline.get_id())
        client.wait_for_status(pipeline.get_id(), IPipeline.STATUS_RUNNING)
        self.logger.info(f'{pipeline.get_id()} pipeline is running')
        if wait_for_sending_data:
            try:
                if self._wait_for_sending_data(pipeline):
                    self.logger.info(f'{pipeline.get_id()} pipeline is sending data')
                else:
                    self.logger.info(f'{pipeline.get_id()} pipeline did not send any data')
            except PipelineException as e:
                self.logger.error(str(e))

    def _update_pipeline(self, pipeline: IPipeline):
        if pipeline.get_offset():
            self._client(pipeline).post_pipeline_offset(pipeline.get_id(), json.loads(pipeline.get_offset()))
        return self._client(pipeline).update_pipeline(pipeline.get_id(), pipeline.get_config())

    def _wait_for_sending_data(self, pipeline: IPipeline) -> bool:
        tries = 5
        initial_delay = 2
        for i in range(1, tries + 1):
            response = self.get_pipeline_metrics(pipeline)
            stats = {
                'in': response['counters']['pipeline.batchInputRecords.counter']['count'],
                'out': response['counters']['pipeline.batchOutputRecords.counter']['count'],
                'errors': response['counters']['pipeline.batchErrorRecords.counter']['count'],
            }
            if stats['out'] > 0 and stats['errors'] == 0:
                return True
            if stats['errors'] > 0:
                raise PipelineException(f"Pipeline {pipeline.get_id()} has {stats['errors']} errors")
            delay = initial_delay ** i
            if i == tries:
                self.logger.warning(
                    f'Pipeline {pipeline.get_id()} did not send any data. Received number of records - {stats["in"]}')
                return False
            self.logger.info(
                f'Waiting for pipeline `{pipeline.get_id()}` to send data. Check again after {delay} seconds...')
            time.sleep(delay)

    def validate(self, pipeline: IPipeline):
        return self._client(pipeline).validate(pipeline.get_id())

    def get_pipeline_offset(self, pipeline: IPipeline) -> Optional[str]:
        res = self._client(pipeline).get_pipeline_offset(pipeline.get_id())
        if res:
            return json.dumps(res)
        return None


def _format_timestamp(utc_time) -> str:
    return datetime.utcfromtimestamp(utc_time / 1000).strftime('%Y-%m-%d %H:%M:%S')


def _format(info: dict) -> str:
    keys = ['severity', 'configGroup', 'configName', 'message']
    return ' - '.join([info[key] for key in keys if key in info])


def _transform_logs(logs: dict) -> list:
    transformed = []
    for item in logs:
        if 'message' not in item:
            continue
        transformed.append([item['timestamp'], item['severity'], item['category'], item['message']])
    return transformed


def choose_streamsets(
        pipeline_streamsets: dict[int, int],
        streamsets: list[IStreamSets],
        *, exclude: int = None
) -> IStreamSets:
    def add_empty(s: IStreamSets):
        if s.get_id() not in pipeline_streamsets:
            pipeline_streamsets[s.get_id()] = 0

    # choose streamsets with the lowest number of pipelines
    map(add_empty, streamsets)
    if exclude:
        del pipeline_streamsets[exclude]
    id_ = min(pipeline_streamsets, key=pipeline_streamsets.get)
    return streamsets[id_]


class PipelineException(Exception):
    pass


class StreamsetsException(Exception):
    pass
