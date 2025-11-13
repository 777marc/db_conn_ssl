"""
DB2 (with FQDN) connection via SQLAlchemy + SSL example.

This file shows two safe ways to create an SQLAlchemy Engine for IBM Db2 using SSL:
  - Preferred: build a DSN string and pass it via the `dsn` query parameter (safer for special chars).
  - Alternate: use the ibm_db_sa URL with SSL query params.

Requirements:
  pip install sqlalchemy ibm_db ibm_db_sa python-dotenv

Notes:
  - Replace placeholders or load credentials from environment variables / a secret store.
  - Default DB2 port is usually 50000; use the port your server listens on.
  - Provide the path to the CA certificate used to sign the Db2 server certificate so the client can verify it.
  - You can confirm the connection by running the test_connection() function which queries SYSIBM.SYSDUMMY1.
"""

import os
import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------------------------------------------------------
# Helper: load credentials from environment (recommended)
# -----------------------------------------------------------------------------
DB2_USER = os.environ.get("DB2_USER", "db2user")
DB2_PASSWORD = os.environ.get("DB2_PASSWORD", "db2pwd")
DB2_HOST = os.environ.get("DB2_HOST", "db2-prod.example.com")  # <-- FQDN
DB2_PORT = int(os.environ.get("DB2_PORT", 50000))
DB2_DATABASE = os.environ.get("DB2_DATABASE", "MYDB")
DB2_CA_CERT = os.environ.get("DB2_CA_CERT", "/path/to/ca_certificate.pem")  # local CA cert file
DB2_CONNECT_TIMEOUT = int(os.environ.get("DB2_CONNECT_TIMEOUT", 10))


# -----------------------------------------------------------------------------
# Method A (recommended): Build a DB2 "DSN" and pass it through the URL.
# This is how you can include all ibm_db connection attributes (Security=SSL, SSLServerCertificate, ...)
# -----------------------------------------------------------------------------
def create_engine_with_dsn(user: str,
                           password: str,
                           host: str,
                           port: int,
                           database: str,
                           ca_cert_path: str | None = None,
                           connect_timeout: int = 10):
    """
    Create an SQLAlchemy engine using a DSN string (recommended for complex params).
    """
    # DSN attributes for ibm_db: DATABASE, HOSTNAME, PORT, PROTOCOL, UID, PWD, Security, SSLServerCertificate
    dsn_attrs = [
        f"DATABASE={database}",
        f"HOSTNAME={host}",
        f"PORT={str(port)}",
        "PROTOCOL=TCPIP",
        f"UID={user}",
        f"PWD={password}",
        "Security=SSL",
        f"CONNECTTIMEOUT={str(connect_timeout)}",
    ]

    if ca_cert_path:
        # SSLServerCertificate points to the PEM file (CA certificate) used to verify the server cert
        dsn_attrs.append(f"SSLServerCertificate={ca_cert_path}")

    dsn = ";".join(dsn_attrs)
    # URL-encode the DSN and pass via the ?dsn=... query parameter
    encoded_dsn = urllib.parse.quote_plus(dsn)
    url = f"ibm_db_sa:///?dsn={encoded_dsn}"

    # You can tune pool settings via create_engine() params
    # Note: timeout is specified in the DSN string above as CONNECTTIMEOUT
    engine = create_engine(
        url,
        pool_size=5,
        max_overflow=10,
        future=True,  # use SQLAlchemy 1.4+ style
    )
    return engine


# -----------------------------------------------------------------------------
# Method B (alternate): Use ibm_db_sa URL with query parameters
# (works for many cases, but special characters in passwords or params may need encoding)
# -----------------------------------------------------------------------------
def create_engine_with_url_params(user: str,
                                  password: str,
                                  host: str,
                                  port: int,
                                  database: str,
                                  ca_cert_path: str | None = None,
                                  connect_timeout: int = 10):
    """
    Create an SQLAlchemy engine using an ibm_db_sa URL and SSL query params.
    Example URL: ibm_db_sa://user:pwd@host:port/database?security=SSL&SSLServerCertificate=/path/ca.pem
    """
    base = f"ibm_db_sa://{urllib.parse.quote_plus(user)}:{urllib.parse.quote_plus(password)}@{host}:{port}/{database}"
    params = {"security": "SSL", "connecttimeout": str(connect_timeout)}
    if ca_cert_path:
        # param name as commonly used by ibm_db driver
        params["SSLServerCertificate"] = ca_cert_path

    qs = "&".join(f"{k}={urllib.parse.quote_plus(v)}" for k, v in params.items())
    url = base + ("?" + qs if qs else "")

    engine = create_engine(
        url,
        pool_size=5,
        max_overflow=10,
        future=True,
    )
    return engine


# -----------------------------------------------------------------------------
# Test helper
# -----------------------------------------------------------------------------
def test_connection(engine) -> bool:
    """
    Run a minimal query to verify the connection is alive and SSL-enabled on the server-side.
    Db2 provides SYSIBM.SYSDUMMY1 for tests: SELECT 1 FROM SYSIBM.SYSDUMMY1
    """
    try:
        with engine.connect() as conn:
            # Simple scalar check
            result = conn.execute(text("SELECT 1 FROM SYSIBM.SYSDUMMY1"))
            val = result.scalar()
            print("Connection test returned:", val)
        return True
    except SQLAlchemyError as e:
        print("Connection test failed:", str(e))
        return False


# -----------------------------------------------------------------------------
# Example usage when running this file directly
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Recommended: use the DSN method
    engine = create_engine_with_dsn(
        user=DB2_USER,
        password=DB2_PASSWORD,
        host=DB2_HOST,
        port=DB2_PORT,
        database=DB2_DATABASE,
        ca_cert_path=DB2_CA_CERT,
        connect_timeout=DB2_CONNECT_TIMEOUT,
    )

    ok = test_connection(engine)
    if ok:
        print("Connected OK to DB2 over SSL.")
    else:
        print("Failed to connect to DB2. See error output above.")