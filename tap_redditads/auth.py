"""Reddit Ads Authentication."""

import json
from datetime import datetime
from typing import Optional
from base64 import b64encode

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import Stream as RESTStreamBase

class RedditAuthenticator(OAuthAuthenticator):
    token_endpoint = "https://www.reddit.com/api/v1/access_token"
    auth_endpoint = "https://www.reddit.com/api/v1/authorize"

    @property
    def oauth_request_body(self) -> dict:
        return {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
        }

    @property
    def oauth_request_headers(self) -> dict:
        client_id = self.config["client_id"]
        client_secret = self.config["client_secret"]
        credentials = f"{client_id}:{client_secret}".encode()
        basic_auth = b64encode(credentials).decode()

        return {
            "Authorization": f"Basic {basic_auth}",
            "User-Agent": self.config["user_agent"],
            "Content-Type": "application/x-www-form-urlencoded"
        }


    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`."""
        request_time = datetime.utcnow()
        auth_request_payload = self.oauth_request_body

        # Make the POST request with headers
        token_response = requests.post(
            self.token_endpoint,  # Correct endpoint for token refresh
            headers=self.oauth_request_headers,  # Use the correct headers
            data=auth_request_payload,  # Send the request body
            timeout=60,
        )

        try:
            token_response.raise_for_status()
        except requests.HTTPError as ex:
            print(f"token response: {token_response.json()}")
            msg = f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            raise RuntimeError(msg) from ex

        self.logger.info("OAuth authorization attempt was successful.")

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expiration = token_json.get("expires_in", self._default_expiration)
        self.expires_in = int(expiration) if expiration else None
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time