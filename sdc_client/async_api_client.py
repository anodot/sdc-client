import asyncio

from sdc_client.base_api_client import _BaseStreamSetsApiClient, ApiClientException, UnauthorizedException
from sdc_client.interfaces import IStreamSets

import aiohttp

MAX_TRIES = 3


def async_endpoint(func):
    """
    logs errors and returns json response
    """
    async def wrap(*args, **kwargs):
        async with args[0].session:
            for i in range(MAX_TRIES):
                try:
                    res = await func(*args, **kwargs)
                    res.raise_for_status()
                    if res.text:
                        return await res.json()
                except aiohttp.ClientConnectionError:
                    if i == MAX_TRIES - 1:
                        raise
                    asyncio.sleep(2 ** i)
                    continue
                except aiohttp.ClientError():
                    raise
                    # TODO parse error responce
            return
    return wrap


class _AsyncStreamSetsApiClient(_BaseStreamSetsApiClient):
    def __init__(self, streamsets_: IStreamSets):
        super().__init__(streamsets_)

    def _get_session(self, streamsets_: IStreamSets):
        session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(
                login=streamsets_.get_username(),
                password=streamsets_.get_password())
        )
        session.headers.update({'X-Requested-By': 'sdc'})
        return session

    @async_endpoint
    async def create_pipeline(self, name: str):
        return await super().create_pipeline(name)

    @async_endpoint
    async def update_pipeline(self, pipeline_id: str, pipeline_config: dict):
        return await super().update_pipeline(pipeline_id, pipeline_config)

    @async_endpoint
    async def start_pipeline(self, pipeline_id: str):
        return await super().start_pipeline(pipeline_id)

    @async_endpoint
    async def stop_pipeline(self, pipeline_id: str):
        return await super().stop_pipeline(pipeline_id)

    @async_endpoint
    async def force_stop(self, pipeline_id: str):
        return await super().force_stop(pipeline_id)

    @async_endpoint
    async def get_pipelines(self, order_by='NAME', order='ASC', label=None):
        return await super().get_pipelines(order_by, order, label)

    @async_endpoint
    async def get_pipeline_statuses(self):
        return await super().get_pipeline_statuses()

    @async_endpoint
    async def delete_pipeline(self, pipeline_id: str):
        return await super().delete_pipeline(pipeline_id)

    @async_endpoint
    async def get_pipeline_logs(self, pipeline_id: str, severity: str = None):
        return await super().get_pipeline_logs(pipeline_id, severity)

    @async_endpoint
    async def get_pipeline(self, pipeline_id: str):
        return await super().get_pipeline(pipeline_id)

    @async_endpoint
    async def get_pipeline_status(self, pipeline_id: str):
        return await super().get_pipeline_status(pipeline_id)

    @async_endpoint
    async def get_pipeline_history(self, pipeline_id: str):
        return await super().get_pipeline_history(pipeline_id)

    @async_endpoint
    async def get_pipeline_metrics(self, pipeline_id: str):
        return await super().get_pipeline_metrics(pipeline_id)

    @async_endpoint
    async def reset_pipeline(self, pipeline_id: str):
        return await super().reset_pipeline(pipeline_id)

    @async_endpoint
    async def get_pipeline_offset(self, pipeline_id: str):
        return await super().get_pipeline_offset(pipeline_id)

    @async_endpoint
    async def post_pipeline_offset(self, pipeline_id: str, offset: dict):
        return await super().post_pipeline_offset(pipeline_id, offset)

    @async_endpoint
    async def validate(self, pipeline_id: str):
        return await super().validate(pipeline_id)

    @async_endpoint
    async def create_preview(self, pipeline_id: str):
        return await super().create_preview(pipeline_id)

    @async_endpoint
    async def get_preview(self, pipeline_id: str, previewer_id: str):
        return await super().get_preview(pipeline_id, previewer_id)

    @async_endpoint
    async def get_preview_status(self, pipeline_id: str, previewer_id: str):
        return await super().get_preview_status(pipeline_id, previewer_id)

    @async_endpoint
    async def get_jmx(self, query: str):
        return await super().get_jmx(query)
