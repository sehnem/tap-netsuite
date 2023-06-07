"""Netsuite tap class."""

from typing import List
from xml.dom import minidom

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_netsuite.client import NetsuiteStream
from tap_netsuite.constants import CUSTOM_SEARCH_FIELDS, SEARCH_ONLY_FIELDS
from tap_netsuite.exceptions import TypeNotFound


class TapNetsuite(Tap):
    """Netsuite tap class."""

    name = "tap-netsuite"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "ns_account",
            th.StringType,
            required=True,
            description="The netsuite account code",
        ),
        th.Property(
            "ns_consumer_key",
            th.StringType,
            required=True,
            description="The netsuite account code consumer key",
        ),
        th.Property(
            "ns_consumer_secret",
            th.StringType,
            required=True,
            description="The netsuite account code consumer secret",
        ),
        th.Property(
            "ns_token_key",
            th.StringType,
            required=True,
            description="The netsuite account code token key",
        ),
        th.Property(
            "ns_token_secret",
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
            name = name[0].upper() + name[1:]
            if name in SEARCH_ONLY_FIELDS:
                continue
            types.append(
                {
                    "name": name,
                    "record_type": record_type,
                }
            )
        return types

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        account = self.config["ns_account"].replace("_", "-")
        url = (
            f"https://{account}.suitetalk.api.netsuite.com/"
            "xsd/platform/v2022_2_0/coreTypes.xsd"
        )
        response = requests.get(url)
        types_xml = minidom.parseString(response.text)

        core_types = []
        core_types.extend(self.extract_xml_types(types_xml, "GetAllRecordType"))
        core_types.extend(self.extract_xml_types(types_xml, "SearchRecordType"))

        for search_type, types in CUSTOM_SEARCH_FIELDS.items():
            for type_name in types:
                core_types.append(
                    {
                        "name": type_name,
                        "record_type": "SearchRecordType",
                        "search_type_name": search_type,
                    }
                )

        for type_def in core_types:
            try:
                yield type(type_def["name"], (NetsuiteStream,), type_def)(tap=self)
            except TypeNotFound:
                self.logger.info(f"Type {type_def['name']} not found in WSDL.")


if __name__ == "__main__":
    TapNetsuite.cli()
