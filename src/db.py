import os
from impala.dbapi import connect
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Input: IMPALA_* Env-Vars. Output: offene Impala-Connection."""
    host = os.getenv("IMPALA_HOST")
    port = int(os.getenv("IMPALA_PORT", "443"))
    http_path = os.getenv("IMPALA_HTTP_PATH")
    user = os.getenv("IMPALA_USER")
    password = os.getenv("IMPALA_PASSWORD")

    if not host or not user or not password:
        raise RuntimeError(
            "Zugangsdaten fehlen. Bitte trage IMPALA_HOST, IMPALA_USER und "
            "IMPALA_PASSWORD in der .env-Datei ein."
        )

    return connect(
        host=host,
        port=port,
        auth_mechanism="LDAP",
        use_ssl=True,
        use_http_transport=True,
        http_path=http_path,
        user=user,
        password=password,
        timeout=120,
    )
