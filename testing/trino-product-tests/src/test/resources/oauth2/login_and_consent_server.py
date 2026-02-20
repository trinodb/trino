import json
import os
import ssl
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Any
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

HYDRA_ADMIN_URL = os.getenv("HYDRA_ADMIN_URL", "https://hydra:4445")
PORT = os.getenv("PORT", 3000)
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE


class LoginAndConsentServer(BaseHTTPRequestHandler):
    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        if self.path == "/healthz":
            self.send_response(200)
        if self.path.startswith("/login"):
            self.accept_login(params)
            return
        if self.path.startswith("/consent"):
            self.accept_consent(params)
            return
        self.send_error(404, "Not found")

    def accept_login(self, params: Dict[str, list[str]]) -> None:
        login_challenge = params["login_challenge"][0]
        with urlopen(Request(
                method="PUT",
                url=HYDRA_ADMIN_URL + "/oauth2/auth/requests/login/accept?login_challenge=" + login_challenge,
                headers={"Content-Type": "application/json; charset=UTF-8"},
                data=json.dumps({"subject": "foo@bar.com"}).encode()),
                context=SSL_CONTEXT) as response:
            self.send_redirect(response)

    def accept_consent(self, params: Dict[str, list[str]]) -> None:
        consent_challenge = params["consent_challenge"][0]
        consent_request = self.get_consent_request(consent_challenge)
        with urlopen(Request(
                method="PUT",
                url=HYDRA_ADMIN_URL + "/oauth2/auth/requests/consent/accept?consent_challenge=" + consent_challenge,
                headers={"Content-Type": "application/json; charset=UTF-8"},
                data=json.dumps({
                    "grant_scope": consent_request["requested_scope"],
                    "grant_access_token_audience": consent_request["requested_access_token_audience"],
                    "session": {
                        "access_token": {
                            "groups": ["admin", "public"]
                        }
                    }}).encode()),
                context=SSL_CONTEXT) as response:
            self.send_redirect(response)

    @staticmethod
    def get_consent_request(consent_challenge: str) -> Dict[str, Any]:
        with urlopen(Request(
                method="GET",
                url=HYDRA_ADMIN_URL + "/oauth2/auth/requests/consent?consent_challenge=" + consent_challenge),
                context=SSL_CONTEXT) as response:
            return json.load(response)

    def send_redirect(self, response):
        body = json.load(response)
        self.send_response(302)
        self.send_header("Location", body["redirect_to"])
        self.end_headers()


if __name__ == "__main__":
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, LoginAndConsentServer)
    httpd.serve_forever()
