from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from zeep.helpers import serialize_object


class NetsuitePaginator(BaseAPIPaginator):
    _search_id = None

    def get_next(self, response):
        response = serialize_object(response)

        self._value = next(
            extract_jsonpath("$.body.searchResult.pageIndex", input=response)
        )
        self._page_count = next(
            extract_jsonpath("$.body.searchResult.totalPages", input=response)
        )
        self._search_id = next(
            extract_jsonpath("$.body.searchResult.searchId", input=response)
        )

        if self._value is None or self._value >= self._page_count:
            self._finished = True
            return None

        return self._value + 1

    @property
    def current_value(self):
        """Get the current pagination value.

        Returns:
            Current page value.
        """
        return dict(searchId=self._search_id, pageIndex=self._value)
