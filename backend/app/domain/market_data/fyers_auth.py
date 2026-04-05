from __future__ import annotations

from typing import Optional
from fyers_apiv3 import fyersModel


class FyersAuth:
    def __init__(self, client_id: str, secret_key: str, redirect_uri: str):
        self.client_id = client_id
        self.secret_key = secret_key
        self.redirect_uri = redirect_uri

    # =========================
    # LOGIN URL (v3 via SDK)
    # =========================
    def generate_login_url(self) -> str:
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        return session.generate_authcode()

    # =========================
    # ACCESS TOKEN (v3 via SDK)
    # =========================
    def generate_access_token(self, auth_code: str) -> Optional[str]:
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code",
            )

            session.set_token(auth_code)
            response = session.generate_token()

            if not isinstance(response, dict):
                return None

            access_token = response.get("access_token")
            return access_token if isinstance(access_token, str) else None

        except Exception:
            return None

    # =========================
    # CLIENT FACTORY
    # =========================
    def get_client(self, access_token: str):
        return fyersModel.FyersModel(
            client_id=self.client_id,
            token=access_token,
            is_async=False,
            log_path="",
        )
