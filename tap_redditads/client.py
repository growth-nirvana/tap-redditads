"""REST client handling, including RedditAdsStream base class."""

import copy
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse, parse_qs

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_redditads.auth import RedditAuthenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class CustomHATEOASPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        full_url = response.json().get("pagination", {}).get("next_url")
        if not full_url:
            return None
        parsed = urlparse(full_url)
        query_params = parse_qs(parsed.query)
        return query_params.get("page.token", [None])[0]  # just the token

class RedditAdsStream(RESTStream):
    name = "reddit_ads_stream"
    records_jsonpath = "$.data[*]"

    @property
    def url_base(self) -> str:
        url: str = self.config["api_url"]
        account_id: str = self.config["account_id"]
        return f"{url}/{account_id}"

    @property
    def authenticator(self) -> RedditAuthenticator:
        return RedditAuthenticator(stream=self)

    def get_new_paginator(self):
        return CustomHATEOASPaginator()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        return {"page.token": next_page_token} if next_page_token else {}
        
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())