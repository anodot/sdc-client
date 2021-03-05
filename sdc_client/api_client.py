import json
import os
import time
import urllib.parse
import inject
import requests
from sdc_client.interfaces import ILogger, IStreamSets

MAX_TRIES = 3
PREVIEW_TIMEOUT = os.environ.get('STREAMSETS_PREVIEW_TIMEOUT', 30000)


def endpoint(func):
    """
    logs errors and returns json response
    """

    def wrapper(*args, **kwargs):
        for i in range(MAX_TRIES):
            try:
                res = func(*args, **kwargs)
                res.raise_for_status()
                if res.text:
                    return res.json()
            except requests.ConnectionError:
                if i == MAX_TRIES - 1:
                    raise
                continue
            except requests.exceptions.HTTPError:
                if res.text:
                    _parse_error_response(res)
                raise
            return

    return wrapper


def _parse_error_response(result):
    try:
        response = result.json()
    except json.decoder.JSONDecodeError:
        raise ApiClientException(result.text)

    if result.status_code == 401:
        raise UnauthorizedException('Unauthorized')

    raise ApiClientException(
        response['RemoteException']['message'],
        response['RemoteException']['exception'],
    )


class _StreamSetsApiClient:
    logger = inject.attr(ILogger)

    def __init__(self, streamsets_: IStreamSets):
        self.base_url = streamsets_.get_url()
        self.session = requests.Session()
        self.session.keep_alive = False
        self.session.auth = (streamsets_.get_username(), streamsets_.get_password())
        self.session.headers.update({'X-Requested-By': 'sdc'})

    def _build_url(self, *args):
        return urllib.parse.urljoin(self.base_url, '/'.join(['/rest/v1', *args]))

    @endpoint
    def create_pipeline(self, name: str):
        self.logger.info(f'Create pipeline `{name}` in `{self.base_url}`')
        return self.session.put(self._build_url('pipeline', name))

    @endpoint
    def update_pipeline(self, pipeline_id: str, pipeline_config: dict):
        self.logger.info(f'Update pipeline `{pipeline_id}` in `{self.base_url}`')
        return self.session.post(self._build_url('pipeline', pipeline_id), json=pipeline_config)

    @endpoint
    def start_pipeline(self, pipeline_id: str):
        self.logger.info(f'Start pipeline `{pipeline_id}`')
        return self.session.post(self._build_url('pipeline', pipeline_id, 'start'))

    @endpoint
    def stop_pipeline(self, pipeline_id: str):
        self.logger.info(f'Stop pipeline `{pipeline_id}`')
        return self.session.post(self._build_url('pipeline', pipeline_id, 'stop'))

    @endpoint
    def force_stop(self, pipeline_id: str):
        self.logger.info(f'Force stop pipeline `{pipeline_id}`')
        return self.session.post(self._build_url('pipeline', pipeline_id, 'forceStop'))

    @endpoint
    def get_pipelines(self, order_by='NAME', order='ASC', label=None):
        self.logger.info('Get pipelines')
        params = {'orderBy': order_by, 'order': order}
        if label:
            params['label'] = label
        return self.session.get(self._build_url('pipelines'), params=params)

    @endpoint
    def get_pipeline_statuses(self) -> requests.Response:
        self.logger.info('Get pipeline statuses')
        return self.session.get(self._build_url('pipelines', 'status'))

    @endpoint
    def delete_pipeline(self, pipeline_id: str):
        self.logger.info(f'Delete pipeline `{pipeline_id}` from `{self.base_url}`')
        return self.session.delete(self._build_url('pipeline', pipeline_id))

    @endpoint
    def get_pipeline_logs(self, pipeline_id: str, severity: str = None):
        self.logger.info(f'Get pipeline logs: `{pipeline_id}`, logging severity:{severity}')
        params = {'pipeline': pipeline_id, 'endingOffset': -1}
        if severity:
            params['severity.value'] = severity
        return self.session.get(self._build_url('system', 'logs'), params=params)

    @endpoint
    def get_pipeline(self, pipeline_id: str):
        self.logger.info(f'Get pipeline `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id))

    @endpoint
    def get_pipeline_status(self, pipeline_id: str):
        self.logger.info(f'Get pipeline status `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'status'))

    @endpoint
    def get_pipeline_history(self, pipeline_id: str):
        self.logger.info(f'Get pipeline history `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'history'))

    @endpoint
    def get_pipeline_metrics(self, pipeline_id: str):
        self.logger.info(f'Get pipeline metrics `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'metrics'))

    @endpoint
    def reset_pipeline(self, pipeline_id: str):
        self.logger.info(f'Reset pipeline `{pipeline_id}`')
        return self.session.post(self._build_url('pipeline', pipeline_id, 'resetOffset'))

    @endpoint
    def get_pipeline_offset(self, pipeline_id: str):
        return self.session.get(self._build_url('pipeline', pipeline_id, 'committedOffsets'))

    @endpoint
    def post_pipeline_offset(self, pipeline_id: str, offset: dict):
        return self.session.post(self._build_url('pipeline', pipeline_id, 'committedOffsets'), json=offset)

    @endpoint
    def validate(self, pipeline_id: str):
        self.logger.info(f'Validate pipeline `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'validate'),
                                params={'timeout': PREVIEW_TIMEOUT})

    @endpoint
    def create_preview(self, pipeline_id: str):
        self.logger.info(f'Create pipeline `{pipeline_id}` preview')
        return self.session.post(self._build_url('pipeline', pipeline_id, 'preview'),
                                 params={'timeout': PREVIEW_TIMEOUT})

    @endpoint
    def get_preview(self, pipeline_id: str, previewer_id: str):
        self.logger.info(f'Get preview `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'preview', previewer_id))

    @endpoint
    def get_preview_status(self, pipeline_id: str, previewer_id: str):
        self.logger.info(f'Get preview status `{pipeline_id}`')
        return self.session.get(self._build_url('pipeline', pipeline_id, 'preview', previewer_id, 'status'))

    def wait_for_preview(self, pipeline_id: str, preview_id: str) -> (list, list):
        tries = 6
        initial_delay = 2
        for i in range(1, tries + 1):
            response = self.get_preview_status(pipeline_id, preview_id)
            if response['status'] == 'TIMED_OUT':
                raise ApiClientException(f'No data. Connection timed out')

            # todo constants
            if response['status'] not in [
                'VALIDATING', 'CREATED', 'RUNNING', 'STARTING', 'FINISHING', 'CANCELLING', 'TIMING_OUT'
            ]:
                break

            delay = initial_delay ** i
            if i == tries:
                return [], []
            self.logger.info(f'Waiting for data. Check again after {delay} seconds...')
            time.sleep(delay)

        preview_data = self.get_preview(pipeline_id, preview_id)

        errors = []
        if preview_data['status'] == 'RUN_ERROR':
            errors.append(preview_data['message'])
        if preview_data['issues']:
            for stage, data in preview_data['issues']['stageIssues'].items():
                for issue in data:
                    errors.append(issue['message'])
            for issue in preview_data['issues']['pipelineIssues']:
                errors.append(issue['message'])

        if preview_data['batchesOutput']:
            for batch in preview_data['batchesOutput']:
                for stage in batch:
                    if stage['errorRecords']:
                        for record in stage['errorRecords']:
                            errors.append(record['header']['errorMessage'])

        return preview_data, errors

    @endpoint
    def get_pipeline_errors(self, pipeline_id: str, stage_name):
        self.logger.info(f'Get pipeline `{pipeline_id}` errors')
        return self.session.get(
            self._build_url('pipeline', pipeline_id, 'errorRecords'),
            params={'stageInstanceName': stage_name}
        )

    @endpoint
    def get_jmx(self, query: str):
        return self.session.get(self._build_url('system', 'jmx'), params={'qry': query})

    def wait_for_status(self, pipeline_id: str, status: str):
        tries = 5
        initial_delay = 3
        for i in range(1, tries + 1):
            response = self.get_pipeline_status(pipeline_id)
            if response['status'] == status:
                return True
            delay = initial_delay ** i
            if i == tries:
                raise PipelineFreezeException(
                    f"Pipeline `{pipeline_id}` is still {response['status']} after {tries} tries"
                )
            self.logger.info(f"Pipeline `{pipeline_id}` is {response['status']}. Check again after {delay} seconds...")
            time.sleep(delay)


class ApiClientException(Exception):
    def __init__(self, message: str, exception_type: str = ''):
        self.exception_type = exception_type
        self.message = message


class UnauthorizedException(Exception):
    def __init__(self, message: str):
        self.message = message
        self.exception_type = ''


class PipelineFreezeException(Exception):
    pass
