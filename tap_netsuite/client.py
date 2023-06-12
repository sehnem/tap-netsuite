"""Custom client handling, including NetsuiteStream base class."""

import base64
import hashlib
import hmac
import random
from datetime import datetime
from decimal import Decimal
from time import time
from typing import Iterable, Optional

import backoff
from backports.cached_property import cached_property
from memoization import cached
from pendulum import parse
from singer import Transformer
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import Stream
from zeep import Client
from zeep.cache import SqliteCache
from zeep.helpers import serialize_object
from zeep.transports import Transport

from tap_netsuite.constants import REPLICATION_KEYS, RETRYABLE_ERRORS
from tap_netsuite.exceptions import TypeNotFound


class NetsuiteStream(Stream):
    """Stream class for Netsuite streams."""

    page_size = 500
    primary_keys = ["internalId"]
    search_type_name = None
    valid_requests = ["getAllResult", "searchResult", "searchMoreWithIdResult"]

    @cached_property
    def account(self):
        return self.config["ns_account"].replace("_", "-")

    @cached_property
    def wsdl_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "wsdl/v2022_2_0/netsuite.wsdl"
        )

    @cached_property
    def datacenter_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "services/NetSuitePort_2022_2"
        )

    @property
    def client(self):
        if self.config["cache_wsdl"]:
            path = "cache.db"
            timeout = 2592000
            cache = SqliteCache(path=path, timeout=timeout)
            transport = Transport(cache=cache)
            return Client(self.wsdl_url, transport=transport)
        return Client(self.wsdl_url)

    @cached_property
    def service_proxy(self):
        proxy_url = "{urn:platform_2022_2.webservices.netsuite.com}NetSuiteBinding"
        return self.client.create_service(proxy_url, self.datacenter_url)

    def search_client(self, type_name):
        for ns_type in self.client.wsdl.types.types:
            if ns_type.name and ns_type.name == type_name:
                return ns_type
        raise TypeNotFound(f"Type {type_name} not found in WSDL")

    @cached_property
    def ns_type(self):
        return self.search_client(self.name)

    @cached_property
    def search_type(self):
        search_type_name = self.search_type_name or self.name + "SearchBasic"
        return self.search_client(search_type_name)

    def generate_token_passport(self):
        consumer_key = self.config["ns_consumer_key"]
        consumer_secret = self.config["ns_consumer_secret"]
        token_key = self.config["ns_token_key"]
        token_secret = self.config["ns_token_secret"]
        account = self.config["ns_account"]

        nonce = "".join([str(random.randint(0, 9)) for _ in range(20)])
        timestamp = str(int(datetime.now().timestamp()))
        key = f"{consumer_secret}&{token_secret}".encode(encoding="ascii")
        msg = "&".join([account, consumer_key, token_key, nonce, timestamp])
        msg = msg.encode(encoding="ascii")

        # compute the signature
        hashed_value = hmac.new(key, msg=msg, digestmod=hashlib.sha256)
        dig = hashed_value.digest()
        signature_value = base64.b64encode(dig).decode()

        passport_signature = self.search_client("TokenPassportSignature")
        signature = passport_signature(signature_value, algorithm="HMAC-SHA256")

        passport = self.search_client("TokenPassport")
        return passport(
            account=account,
            consumerKey=consumer_key,
            token=token_key,
            nonce=nonce,
            timestamp=timestamp,
            signature=signature,
        )

    def build_headers(self, include_search_preferences: bool = False):
        soapheaders = {}
        soapheaders["tokenPassport"] = self.generate_token_passport()
        if include_search_preferences:
            search_preferences = self.search_client("SearchPreferences")
            preferences = {
                "bodyFieldsOnly": True,
                "pageSize": self.page_size,
                "returnSearchColumns": True,
            }
            soapheaders["searchPreferences"] = search_preferences(**preferences)
        return soapheaders

    @backoff.on_exception(backoff.expo, RetriableAPIError, max_tries=5, factor=2)
    def request(self, name, *args, **kwargs):
        method = getattr(self.service_proxy, name)
        # call the service:
        is_search = name == "search"
        headers = self.build_headers(include_search_preferences=is_search)

        request_start_time = time()
        response = method(*args, _soapheaders=headers, **kwargs)
        request_duration = time() - request_start_time

        response_body_attrs = list(vars(response.body)["__values__"].keys())
        request_type = next(k for k in response_body_attrs if k in self.valid_requests)

        result = getattr(response.body, request_type)

        if hasattr(result, "totalRecords"):
            page_size = result.totalRecords
        elif result.totalPages == result.pageIndex:
            page_size = result.totalRecords - (result.pageIndex - 1) * result.pageSize
        else:
            page_size = result.pageSize

        request_status = "SUCCESS" if result.status.isSuccess else "ERROR"
        extra_tags = dict(page_size=page_size)
        metric = {
            "type": "timer",
            "metric": "request_duration",
            "value": round(request_duration, 4),
            "tags": {
                "object": self.name,
                "status": request_status,
            },
        }
        self._write_metric_log(metric=metric, extra_tags=extra_tags)

        self.validate_response(result)
        return result

    def get_all_records(self, context):
        type_name = self.name[0].lower() + self.name[1:]
        get_all_record = self.search_client("GetAllRecord")
        record = get_all_record(recordType=type_name)
        response = self.request("getAll", record=record)

        records = response["recordList"]["record"]
        return serialize_object(records)

    @cached
    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(start_date)
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_all_paginated(self, context):
        # apply filters
        search_type = self.search_type()
        rk = self.replication_key
        start_date = self.get_starting_time(context)
        if start_date and rk and hasattr(search_type, rk):
            search_date = self.search_client("SearchDateField")
            search_date = search_date(searchValue=start_date, operator="onOrAfter")
            setattr(search_type, rk, search_date)
        if getattr(search_type, "recordType", None):
            search_string = self.search_client("SearchStringField")
            search_type.recordType = search_string(
                searchValue=self.name, operator="contains"
            )

        # request records
        result = self.request("search", searchRecord=search_type)
        total_pages = result.totalPages
        page_index = result.pageIndex
        search_id = result.searchId

        if total_pages == 0:
            return []

        for record in result["recordList"]["record"]:
            yield serialize_object(record)

        while total_pages > page_index:
            next_page = page_index + 1
            result = self.request(
                "searchMoreWithId", searchId=search_id, pageIndex=next_page
            )
            page_index = result.pageIndex

            for record in result["recordList"]["record"]:
                yield serialize_object(record)

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if self.record_type == "GetAllRecordType":
            response = self.get_all_records(context)
        elif self.record_type == "SearchRecordType":
            response = self.get_all_paginated(context)

        def pre_hook(data, _, schema):
            if schema.get("format") == "date-time" and data:
                data = data.isoformat()
            return data

        with Transformer(pre_hook=pre_hook) as transformer:
            for record in response:
                yield transformer.transform(record, self.schema)

    @cached_property
    def schema(self):
        if getattr(self._tap, "input_catalog"):
            streams = self._tap.input_catalog.to_dict()
            streams = (s for s in streams["streams"] if s["tap_stream_id"] == self.name)
            stream_catalog = next(streams, None)
            if stream_catalog:
                return stream_catalog["schema"]
        properties = self.unwrap_zeep(self.ns_type)
        replication_key = next(
            (p for p in properties if p.name.lower() in REPLICATION_KEYS), None
        )
        if replication_key:
            self.replication_key = replication_key.name

        return th.PropertiesList(*properties).to_dict()

    def extract_type(self, type_obj):
        type_cls = type_obj.type.accepted_types[0]

        if hasattr(type_cls, "_xsd_type"):
            field_name = type_cls._xsd_type.name
            properties = self.unwrap_zeep(type_cls._xsd_type)

            if field_name == 'CustomFieldList':
                array_type = th.ArrayType(
                    th.ObjectType(
                        th.Property("internalId",th.StringType),
                        th.Property("scriptId",th.StringType),
                        th.Property("value",th.CustomType({"anyOf": [
                                {
                                    "type":["array","null"],
                                    "items": { 
                                        "properties": { 
                                            "internalId": { 
                                                "type":["string","null"]
                                            },
                                            "externalId": { 
                                                "type":["string","null"]
                                            },
                                            "name": { 
                                                "type":["string","null"]
                                            },
                                            "typeId": { 
                                                "type":["string","null"]
                                            },
                                        },
                                        "type":["object","null"]
                                    },
                                },
                                {
                                    "type":["string","boolean","number","integer","null"]
                                }

                            ]
                        })
                        ),
                    )
                )
                properties.append(th.Property("customField",array_type))

            object = th.ObjectType(*properties)
            if getattr(type_obj, "accepts_multiple", None):
                object = th.ArrayType(object)
            return th.Property(field_name, object)

        if type_cls is str:
            property = th.StringType
        elif type_cls is bool:
            property = th.BooleanType
        elif type_cls is int:
            property = th.IntegerType
        elif type_cls is Decimal:
            property = th.NumberType
        elif type_cls is datetime:
            property = th.DateTimeType
        else:
            self.logger.error(f"Unsupported type {type_cls}")
            return None

        return property

    def unwrap_zeep(self, ns_schema_type):
        properties = []
        fields_list = ns_schema_type.attributes + ns_schema_type.elements

        for field_name, type_obj in fields_list:
            schema_type = self.extract_type(type_obj)
            if type_obj.name in ["nullFieldList"]:
                continue
            if schema_type:
                property = th.Property(field_name, schema_type)
                properties.append(property)
        return properties

    def validate_response(self, result) -> None:
        """Validate zeep response."""
        if not result.status.isSuccess:
            status = result.status.statusDetail[0]
            if status.code in RETRYABLE_ERRORS:
                msg = self.response_error_message(status)
                raise RetriableAPIError(msg, status)
            else:
                msg = self.response_error_message(status)
                raise FatalAPIError(msg)

    def response_error_message(self, status) -> str:
        """Build error message for invalid http statuses."""
        return f'{status.code} error for {self.name}: "{status.message}"'
