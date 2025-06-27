"""Stream type classes for tap-redditads."""

from __future__ import annotations

import json
import sys
import typing as t
from datetime import datetime, timedelta
import dateutil.parser
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_redditads.client import RedditAdsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

def normalize_timestamp(ts_str):
    # Parse the timestamp (handles both full datetime and date-only strings)
    dt = dateutil.parser.isoparse(ts_str)
    # Subtract one day
    dt = dt - timedelta(days=1)
    # Set time to start of hour
    dt = dt.replace(minute=0, second=0, microsecond=0)
    # Format as ISO 8601 with UTC time
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def extract_fields_and_metrics(breakdown_columns:list[str], config_file: Path) -> list[str]:
    """Extract metrics from post request json."""

    with config_file.open() as f:
        config_data = json.load(f)

    properties = config_data.get("properties", {})
    all_fields = list(properties.keys())

    exclude_columns = [col.lower() for col in breakdown_columns + ["metrics_updated_at"]]

    # Exclude breakdowns and uppercase the rest for the POST request
    fields = [field.upper() for field in all_fields if field not in exclude_columns]

    return fields

class ReportCampaignDateStream(RedditAdsStream):
    """Define Report report broken down by Campaign and Date."""
    name = "reports"
    path = "/reports"
    primary_keys = ["date", "campaign_id"]
    replication_key = "metrics_updated_at" # change to incremental
    schema_filepath = SCHEMAS_DIR / "report_by_campaign_date.json"
    rest_method = "POST"

    records_jsonpath = "$.data.metrics[*]"

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """

        now = datetime.utcnow()
        rounded_hour = now.replace(minute=0, second=0, microsecond=0)
        end_date = rounded_hour.strftime('%Y-%m-%dT%H:00:00Z')

        breakdown_columns = ["campaign_id", "date"]

        fields = extract_fields_and_metrics(breakdown_columns, self.schema_filepath)
        
        timestamp = self.get_starting_replication_key_value(context)
        # Metrics may take up to 6 hours to stabilize.

        # replication_key is in the format YYYY-MM-DDT21:23:17.915000+00:00'
        start_date = normalize_timestamp(timestamp)

        payload = {
            "data": {
                "breakdowns": breakdown_columns,
                "fields": fields,
                "starts_at": start_date,
                "ends_at": end_date,
                "time_zone_id": "GMT"
            }
        }

        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json().get("data", {})
        updated_at = data.get("metrics_updated_at")

        for record in data.get("metrics", []):
            record["metrics_updated_at"] = updated_at
            yield record

class AdConversionsReportStream(RedditAdsStream):
    """Reddit Ads Conversions Report stream."""
    name = "ad_conversions_report"
    path = "/reports"
    primary_keys = ["event_name", "account_id", "ad_id", "date"]
    replication_key = "metrics_updated_at"
    rest_method = "POST"
    schema_filepath = SCHEMAS_DIR / "ad_conversions_report.json"

    records_jsonpath = "$.data.metrics[*]"

    def prepare_request_payload(self, context: Context | None, next_page_token: Any | None) -> dict | None:
        now = datetime.utcnow()
        rounded_hour = now.replace(minute=0, second=0, microsecond=0)
        end_date = rounded_hour.strftime('%Y-%m-%dT%H:00:00Z')

        breakdown_columns = ["event_name", "account_id", "ad_id", "date"]

        fields = extract_fields_and_metrics(breakdown_columns, self.schema_filepath)
        start_date = normalize_timestamp(self.get_starting_replication_key_value(context))

        return {
            "data": {
                "breakdowns": breakdown_columns,
                "fields": fields,
                "starts_at": start_date,
                "ends_at": end_date,
                "time_zone_id": "GMT"
            }
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json().get("data", {})
        updated_at = data.get("metrics_updated_at")

        for record in data.get("metrics", []):
            record["metrics_updated_at"] = updated_at
            yield record

class AdReportStream(RedditAdsStream):
    name = "ad_report"
    path = "/reports"
    rest_method = "POST"
    primary_keys = ["account_id", "ad_id", "date"]
    replication_key = "metrics_updated_at"
    schema_filepath = SCHEMAS_DIR / "ad_report.json"
    records_jsonpath = "$.data.metrics[*]"

    def prepare_request_payload(self, context, next_page_token):
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        end_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Start from last replicated
        timestamp = self.get_starting_replication_key_value(context)
        start_date = normalize_timestamp(timestamp)

        breakdown_columns = ["ad_id", "date"]
        fields = extract_fields_and_metrics(breakdown_columns, self.schema_filepath)

        return {
            "data": {
                "breakdowns": breakdown_columns,
                "fields": fields,
                "starts_at": start_date,
                "ends_at": end_date,
                "time_zone_id": "GMT"
            }
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json().get("data", {})
        updated_at = data.get("metrics_updated_at")

        for record in data.get("metrics", []):
            record["metrics_updated_at"] = updated_at
            yield record



class CampaignsStream(RedditAdsStream):
    """Define Campaigns stream."""
    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    rest_method = "GET"
    replication_key = "modified_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

class AdsStream(RedditAdsStream):
    """Define Ads stream."""
    name = "ads"
    path = "/ads"
    primary_keys = ["id"]
    rest_method = "GET"
    replication_key = "modified_at"
    schema_filepath = SCHEMAS_DIR / "ads.json"

class AdGroupsStream(RedditAdsStream):
    """Stream for Ad Groups within an Ad Account."""
    name = "ad_groups"
    path = "/ad_groups"
    primary_keys = ["id"]
    rest_method = "GET"
    replication_key = "modified_at"
    schema_filepath = SCHEMAS_DIR / "ad_groups.json"
    
class BusinessAccountStream(RedditAdsStream):
    name = "business_account"
    path = ""
    schema_filepath = SCHEMAS_DIR / "business_account.json"
    primary_keys = ["id"]
    rest_method = "GET"
    replication_key = "modified_at"




