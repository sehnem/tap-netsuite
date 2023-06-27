"""Custom client handling, including NetsuiteStream base class."""

import base64
import hashlib
import hmac
import random
from datetime import datetime
from decimal import Decimal

from backports.cached_property import cached_property
from memoization import cached
from pendulum import parse
from singer_sdk import typing as th

from tap_netsuite.constants import REPLICATION_KEYS
from tap_netsuite.paginator import NetsuitePaginator
from tap_netsuite.streams.soap import SOAPStream


class NetsuiteStream(SOAPStream):
    """Stream class for Netsuite streams."""

    primary_keys = ["internalId"]
    retry_statuses = [
        "ACCT_TEMP_UNAVAILABLE",
        "BILL_PAY_STATUS_UNAVAILABLE",
        "BILLPAY_SRVC_UNAVAILBL",
        "PAYROLL_IN_PROCESS",
    ]

    @cached_property
    def account(self):
        return self.config["account"].replace("_", "-")

    @cached_property
    def wsdl_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "wsdl/v2022_2_0/netsuite.wsdl"
        )

    @cached_property
    def proxy_url(self):
        return "{urn:platform_2022_2.webservices.netsuite.com}NetSuiteBinding"

    @cached_property
    def service_url(self):
        return (
            f"https://{self.account}.suitetalk.api.netsuite.com/"
            "/services/NetSuitePort_2022_2"
        )

    @cached_property
    def service_proxy(self):
        return self.client.create_service(self.proxy_url, self.service_url)

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

        passport_signature = self.get_client_type("TokenPassportSignature")
        signature = passport_signature(signature_value, algorithm="HMAC-SHA256")

        passport = self.get_client_type("TokenPassport")
        return passport(
            account=account,
            consumerKey=consumer_key,
            token=token_key,
            nonce=nonce,
            timestamp=timestamp,
            signature=signature,
        )

    @cached_property
    def catalog_dict(self):
        if getattr(self._tap, "input_catalog"):
            catalog = self._tap.input_catalog.to_dict()
            return catalog["streams"]
        return {}

    @cached_property
    def schema(self):
        if getattr(self._tap, "input_catalog"):
            streams = self.catalog_dict
            stream = (s for s in streams if s["tap_stream_id"] == self.name)
            stream_catalog = next(stream, None)
            if stream_catalog:
                return stream_catalog["schema"]
        soap_type_obj = self.get_client_type(self.name)
        properties = self.unwrap_zeep(soap_type_obj)
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


class NetsuiteGetAllStream(NetsuiteStream):
    """Stream class for Netsuite streams."""

    soap_service = "getAll"
    records_jsonpath = "$.body.getAllResult.recordList.record[*]"
    status_jsonpath = "$.body.getAllResult.status"
    paginator = NetsuitePaginator

    @property
    def soap_headers(self):
        soapheaders = {}
        soapheaders["tokenPassport"] = self.generate_token_passport()
        return soapheaders

    def prepare_request_body(self, context, next_page_token=None):
        get_all_record = self.get_client_type("GetAllRecord")
        record = get_all_record(recordType=self.soap_type)
        return dict(record=record)


class NetsuiteSearchPaginatedStream(NetsuiteStream):
    """Stream class for Netsuite streams."""

    page_size = 500
    soap_service = "search"
    records_jsonpath = "$.body.searchResult.recordList.record[*]"
    status_jsonpath = "$.body.searchResult.status"
    search_type_name = None

    def get_new_paginator(self):
        return NetsuitePaginator(0)

    @property
    def soap_headers(self):
        soapheaders = {}
        soapheaders["tokenPassport"] = self.generate_token_passport()
        search_preferences = self.get_client_type("SearchPreferences")
        preferences = {
            "bodyFieldsOnly": True,
            "pageSize": self.page_size,
            "returnSearchColumns": True,
        }
        soapheaders["searchPreferences"] = search_preferences(**preferences)
        return soapheaders

    @cached
    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(start_date)
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def prepare_request_body(self, context, next_page_token=None):
        if next_page_token.get("searchId"):
            self.soap_service = "searchMoreWithId"
            return next_page_token

        search_type_name = self.search_type_name or self.name + "SearchBasic"
        search_type = self.get_client_type(search_type_name)()
        rk = self.replication_key
        start_date = self.get_starting_time(context)
        if start_date and rk and hasattr(search_type, rk):
            search_date = self.get_client_type("SearchDateField")
            search_date = search_date(searchValue=start_date, operator="onOrAfter")
            setattr(search_type, rk, search_date)
        if getattr(search_type, "recordType", None):
            search_string = self.get_client_type("SearchStringField")
            search_type.recordType = search_string(
                searchValue=self.name, operator="contains"
            )
        return dict(searchRecord=search_type)
