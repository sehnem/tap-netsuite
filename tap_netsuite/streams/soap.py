"""Abstract base class for API-type streams."""

from __future__ import annotations

import abc
import logging
from functools import cached_property
from time import time
from typing import TYPE_CHECKING, Any, Callable, Generator, Generic, Iterable, TypeVar

import backoff
import requests
from singer_sdk import metrics
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import (
    BaseAPIPaginator,
    JSONPathPaginator,
    SinglePagePaginator,
)
from singer_sdk.streams.core import Stream
from zeep import Client
from zeep.cache import SqliteCache
from zeep.helpers import serialize_object
from zeep.transports import Transport

from tap_netsuite.exceptions import TypeNotFound

if TYPE_CHECKING:
    from backoff.types import Details
    from singer_sdk._singerlib import Schema
    from singer_sdk.plugin_base import PluginBase as TapBaseClass

DEFAULT_PAGE_SIZE = 1000

_TToken = TypeVar("_TToken")


class SOAPStream(Stream, Generic[_TToken], metaclass=abc.ABCMeta):
    """Abstract base class for REST API streams."""

    _page_size: int = DEFAULT_PAGE_SIZE
    _requests_session: requests.Session | None
    soap_service: str | None = None
    soap_type: str | None = None
    wsdl_url: str | None = None

    #: JSONPath expression to extract records from the API response.
    records_jsonpath: str = "$[*]"
    status_jsonpath: str = "$.status"

    #: Response code reference for rate limit retries
    retry_statuses: list[str] = []

    #: Optional JSONPath expression to extract a pagination token from the API response.
    #: Example: `"$.next_page"`
    next_page_token_jsonpath: str | None = None

    # Private constants. May not be supported in future releases:
    _LOG_REQUEST_METRICS: bool = True
    # Disabled by default for safety:
    _LOG_REQUEST_METRIC_URLS: bool = False

    def __init__(
        self,
        tap: TapBaseClass,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap)
        self._soap_headers: dict = {}
        self._client = self.client
        self._compiled_jsonpath = None
        self._next_page_token_compiled_jsonpath = None

    @cached_property
    def client(self):
        if self.config.get("cache_wsdl", False):
            path = "cache.db"
            timeout = 2592000
            cache = SqliteCache(path=path, timeout=timeout)
            transport = Transport(cache=cache)
            return Client(self.wsdl_url, transport=transport)
        return Client(self.wsdl_url)

    @cached_property
    def service_proxy(self):
        return self.client.service

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        result = self.parse_status(response)
        if not result.isSuccess:
            status = result.statusDetail[0]
            if status.code in self.retry_statuses:
                msg = self.response_error_message(status)
                raise RetriableAPIError(msg, status)
            else:
                msg = self.response_error_message(status)
                raise FatalAPIError(msg)

    def response_error_message(self, status) -> str:
        """Build error message for invalid http statuses.

        Args:
            status: the status object from the response.

        Returns:
            str: The error message
        """
        return f'{status.code} error for {self.name}: "{status.message}"'

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Uses a wait generator defined in `backoff_wait_generator` to
        determine backoff behaviour. Try limit is defined in
        `backoff_max_tries`, and will trigger the event defined in
        `backoff_handler` before retrying. Developers may override one or
        all of these methods to provide custom backoff or retry handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            RetriableAPIError,
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
            jitter=self.backoff_jitter,
        )(func)
        return decorator

    def prepare_request_body(
        self,
        context: dict | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        return {}

    @property
    def soap_request_method(self) -> str:
        """Return the SOAP service object used for this stream's API request."""
        return getattr(self.service_proxy, self.soap_service)

    def get_client_type(self, type_name: str):
        for ns_type in self.client.wsdl.types.types:
            if ns_type.name and ns_type.name == type_name:
                return ns_type
        raise TypeNotFound(f"Type {type_name} not found in WSDL")

    def _request(
        self,
        headers: dict | None = None,
        body: dict | None = None,
    ) -> requests.Response:
        """TODO.

        Args:
            prepared_request: TODO
            context: Stream partition or context dictionary.

        Returns:
            TODO
        """

        request_start_time = time()
        response = self.soap_request_method(_soapheaders=headers, **body)
        request_duration = time() - request_start_time

        metric = {
            "metric": metrics.Metric.HTTP_REQUEST_DURATION,
            "value": round(request_duration, 4),
            "tags": {
                "object": self.name,
                # "page_size": self._page_size,
            },
        }
        self._write_request_duration_log(metric=metric)

        return response

    def _write_request_duration_log(
        self,
        metric: dict | None,
        extra_tags: dict | None = None,
    ) -> None:
        """TODO.
        Args:
            endpoint: TODO
            response: TODO
            context: Stream partition or context dictionary.
            extra_tags: TODO
        """
        extra_tags = extra_tags or {}

        point = metrics.Point("timer", **metric)
        self._log_metric(point)

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)

        with metrics.http_request_counter(self.name, self.soap_type) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                headers = self.soap_headers
                body = self.prepare_request_body(
                    context, next_page_token=paginator.current_value
                )
                resp = decorated_request(headers, body)
                request_counter.increment()
                yield from self.parse_response(resp)

                paginator.advance(resp)

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get a fresh paginator for this API endpoint.

        Returns:
            A paginator instance.
        """

        if self.next_page_token_jsonpath:
            return JSONPathPaginator(self.next_page_token_jsonpath)

        return SinglePagePaginator()

    @property
    def soap_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.

        Returns:
            Dictionary of HTTP headers to use as a base for every request.
        """
        result = self._soap_headers
        return result

    # Records iterator

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record

    def parse_status(self, response) -> dict:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        return extract_jsonpath(self.status_jsonpath, input=serialize_object(response))

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        yield from extract_jsonpath(
            self.records_jsonpath, input=serialize_object(response)
        )

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        """The wait generator used by the backoff decorator on request failure.

        See for options:
        https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py

        And see for examples: `Code Samples <../code_samples.html#custom-backoff>`_

        Returns:
            The wait generator
        """
        return backoff.expo(factor=2)

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Returns:
            Number of max retries.
        """
        return 5

    def backoff_jitter(self, value: float) -> float:
        """Amount of jitter to add.

        For more information see
        https://github.com/litl/backoff/blob/master/backoff/_jitter.py

        We chose to default to ``random_jitter`` instead of ``full_jitter`` as we keep
        some level of default jitter to be "nice" to downstream APIs but it's still
        relatively close to the default value that's passed in to make tap developers'
        life easier.

        Args:
            value: Base amount to wait in seconds

        Returns:
            Time in seconds to wait until the next request.
        """
        return backoff.random_jitter(value)

    def backoff_handler(self, details: Details) -> None:
        """Adds additional behaviour prior to retry.

        By default will log out backoff details, developers can override
        to extend or change this behaviour.

        Args:
            details: backoff invocation details
                https://github.com/litl/backoff#event-handlers
        """
        logging.error(
            "Backing off %(wait)0.2f seconds after %(tries)d tries "
            "calling function %(target)s with args %(args)s and kwargs "
            "%(kwargs)s",
            details.get("wait"),
            details.get("tries"),
            details.get("target"),
            details.get("args"),
            details.get("kwargs"),
        )

    def backoff_runtime(
        self,
        *,
        value: Callable[[Any], int],
    ) -> Generator[int, None, None]:
        """Optional backoff wait generator that can replace the default `backoff.expo`.

        It is based on parsing the thrown exception of the decorated method, making it
        possible for response values to be in scope.

        You may want to review :meth:`~singer_sdk.RESTStream.backoff_jitter` if you're
        overriding this function.

        Args:
            value: a callable which takes as input the decorated
                function's thrown exception and determines how
                long to wait.

        Yields:
            The thrown exception
        """
        exception = yield  # type: ignore[misc]
        while True:
            exception = yield value(exception)
