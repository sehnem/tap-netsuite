"""Stream type classes for tap-netsuite."""

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from singer_sdk import typing as th

from tap_netsuite.client import NetsuiteStream


class AccountStream(NetsuiteStream):
    """Define custom stream."""

    name = "Account"
    primary_keys = ["internalId"]
    ns_type = "ns17:Account"


class ClassificationStream(NetsuiteStream):
    """Define custom stream."""

    name = "Classification"
    primary_keys = ["internalId"]
    ns_type = "ns17:Classification"


class DepartmentStream(NetsuiteStream):
    """Define custom stream."""

    name = "Department"
    primary_keys = ["internalId"]
    ns_type = "ns17:Department"


class CurrencyStream(NetsuiteStream):
    """Define custom stream."""

    name = "Currency"
    primary_keys = ["internalId"]
    ns_type = "ns17:Currency"
