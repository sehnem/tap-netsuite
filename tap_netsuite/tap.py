"""Netsuite tap class."""

from typing import List
from xml.dom import minidom

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_netsuite.client import NetsuiteGetAllStream, NetsuiteSearchPaginatedStream
from tap_netsuite.constants import CUSTOM_SEARCH_FIELDS, SEARCH_ONLY_FIELDS


class TapNetsuite(Tap):
    """Netsuite tap class."""

    name = "tap-netsuite"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="The netsuite account code",
        ),
        th.Property(
            "consumer_key",
            th.StringType,
            required=True,
            description="The netsuite account code consumer key",
        ),
        th.Property(
            "consumer_secret",
            th.StringType,
            required=True,
            description="The netsuite account code consumer secret",
        ),
        th.Property(
            "token_key",
            th.StringType,
            required=True,
            description="The netsuite account code token key",
        ),
        th.Property(
            "token_secret",
            th.StringType,
            required=True,
            description="The netsuite account code token secret",
        ),
        th.Property(
            "cache_wsdl",
            th.BooleanType,
            default=True,
            description="If the WSDL should be cached",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def extract_xml_types(self, xml: str, record_type: str) -> List[str]:
        types = []
        type_records = None

        for simple_type in xml.getElementsByTagName("simpleType"):
            if simple_type.getAttribute("name") == record_type:
                type_records = simple_type
                break

        if not type_records:
            return []

        type_records = type_records.getElementsByTagName("restriction")[0]
        type_records = type_records.getElementsByTagName("enumeration")
        type_records = [i.getAttribute("value") for i in type_records]
        for name in type_records:
            soap_type = name
            name = name[0].upper() + name[1:]
            if name in SEARCH_ONLY_FIELDS:
                continue
            types.append(
                {
                    "name": name,
                    "soap_type": soap_type,
                    "record_type": record_type,
                }
            )
        return types

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        account = self.config["account"].replace("_", "-")
        url = (
            f"https://{account}.suitetalk.api.netsuite.com/"
            "xsd/platform/v2022_2_0/coreTypes.xsd"
        )
        response = requests.get(url)
        types_xml = minidom.parseString(response.text)

        for type_def in self.extract_xml_types(types_xml, "GetAllRecordType"):
            yield type(type_def["name"], (NetsuiteGetAllStream,), type_def)(tap=self)

        for type_def in self.extract_xml_types(types_xml, "SearchRecordType"):
            yield type(type_def["name"], (NetsuiteSearchPaginatedStream,), type_def)(
                tap=self
            )

        for search_type, types in CUSTOM_SEARCH_FIELDS.items():
            for type_name in types:
                type_def = {
                    "name": type_name,
                    "record_type": "SearchRecordType",
                    "search_type_name": search_type,
                }
                yield type(
                    type_def["name"], (NetsuiteSearchPaginatedStream,), type_def
                )(tap=self)


if __name__ == "__main__":
    TapNetsuite.cli()
