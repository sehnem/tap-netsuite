"""Custom client handling, including NetsuiteStream base class."""

import base64
import hashlib
import hmac
import random
from datetime import datetime
from decimal import Decimal
from functools import cached_property
from typing import Iterable, Optional

import backoff
from memoization import cached
from pendulum import parse
from singer import Transformer
from singer_sdk import typing as th
from singer_sdk.streams import Stream
from zeep import Client
from zeep.cache import SqliteCache
from zeep.helpers import serialize_object
from zeep.transports import Transport

from tap_netsuite.constants import REPLICATION_KEYS
from tap_netsuite.exceptions import TypeNotFound


class NetsuiteStream(Stream):
    """Stream class for Netsuite streams."""

    page_size = 500
    primary_keys = ["internalId"]
    search_type_name = None

    @cached_property
    def account(self):
        return self.config["account"].replace("_", "-")

    @cached_property
    def wsdl_url(self):
        return f"https://{self.account}.suitetalk.api.netsuite.com/wsdl/v2022_2_0/netsuite.wsdl"

    @cached_property
    def datacenter_url(self):
        return f"https://{self.account}.suitetalk.api.netsuite.com/services/NetSuitePort_2022_2"

    @property
    def client(self):
        path = "cache.db"
        timeout = 2592000
        cache = SqliteCache(path=path, timeout=timeout)
        transport = Transport(cache=cache)
        return Client(self.wsdl_url, transport=transport)

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
        consumer_key = self.config["consumer_key"]
        consumer_secret = self.config["consumer_secret"]
        token_key = self.config["token_key"]
        token_secret = self.config["token_secret"]
        account = self.config["account"]

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

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=3)
    def request(self, name, *args, **kwargs):
        method = getattr(self.service_proxy, name)
        # call the service:
        is_search = name == "search"
        headers = self.build_headers(include_search_preferences=is_search)
        response = method(*args, _soapheaders=headers, **kwargs)
        return response

    def get_all_records(self, context):
        type_name = self.name[0].lower() + self.name[1:]
        get_all_record = self.search_client("GetAllRecord")
        record = get_all_record(recordType=type_name)
        response = self.request("getAll", record=record)
        response = response.body.getAllResult

        status = response.status
        if status.isSuccess:
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
        response = self.request("search", searchRecord=search_type)
        result = response.body.searchResult
        total_pages = result.totalPages
        page_index = result.pageIndex
        search_id = result.searchId

        for record in result["recordList"]["record"]:
            yield serialize_object(record)

        while total_pages > page_index:
            next_page = page_index + 1
            response = self.request(
                "searchMoreWithId", searchId=search_id, pageIndex=next_page
            )
            result = response.body.searchResult
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
            if schema_type:
                property = th.Property(field_name, schema_type)
                properties.append(property)
        return properties