//! TLS helpers for talking to PostgreSQL.
//!
//! PostgreSQL servers require SSL — `NoTls` cannot connect to
//! `*.postgres.database.azure.com`. This module builds a
//! [`tokio_postgres_rustls::MakeRustlsConnect`] backed by the platform's
//! native trust store, and exposes a [`SslMode`] enum parsed from the
//! libpq-style `sslmode=` query parameter.

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::Result;

/// Subset of libpq's `sslmode` we recognise.
///
/// Only the modes that actually change connection behaviour in this crate are
/// represented; the others map to one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Plaintext — no SSL handshake.
    Disable,
    /// SSL handshake but no certificate verification (libpq's `require`).
    Require,
    /// SSL handshake **with** root-CA + hostname verification (libpq's
    /// `verify-full` / `verify-ca`).
    VerifyFull,
}

impl SslMode {
    /// Parse a libpq-style `sslmode=…` value. Unknown values fall back to
    /// [`SslMode::Require`] which matches what most managed Postgres
    /// providers (Azure, RDS) expect.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disable" => Self::Disable,
            "verify-ca" | "verify-full" => Self::VerifyFull,
            _ => Self::Require,
        }
    }

    /// Extract `sslmode=…` from a libpq URI / DSN. Returns
    /// [`SslMode::Require`] when the parameter is absent (matches what
    /// Azure-style hosts expect by default).
    pub fn from_connection_string(conn: &str) -> Self {
        let query = match conn.split_once('?') {
            Some((_, q)) => q,
            None => return Self::Require,
        };
        for kv in query.split('&') {
            if let Some(("sslmode", v)) = kv.split_once('=') {
                return Self::parse(v);
            }
        }
        Self::Require
    }
}

/// Build a `MakeRustlsConnect` honouring the given [`SslMode`].
///
/// Returns `Ok(None)` when the mode is [`SslMode::Disable`] — callers should
/// fall back to `tokio_postgres::NoTls` in that case.
pub fn make_tls_connector(mode: SslMode) -> Result<Option<MakeRustlsConnect>> {
    if mode == SslMode::Disable {
        return Ok(None);
    }

    // Install the default crypto provider (idempotent — second call is a
    // no-op error we deliberately ignore).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // `cert` is already a `CertificateDer<'static>`; ignore individual
        // bad certs rather than failing the whole connection.
        roots.add(cert).ok();
    }

    let config = if mode == SslMode::VerifyFull {
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else {
        // `Require`: TLS, no verification. Use a permissive verifier.
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerification::default()))
            .with_no_client_auth()
    };

    Ok(Some(MakeRustlsConnect::new(config)))
}

/// "Trust everything" rustls verifier used for `sslmode=require` (libpq
/// behaviour: encrypt but don't verify).
#[derive(Debug)]
struct NoCertVerification {
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl Default for NoCertVerification {
    fn default() -> Self {
        Self {
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported.supported_schemes()
    }
}

/// Convenience: open a connection honouring the URL's `sslmode=`.
///
/// Spawns the connection driver task on the current tokio runtime.
pub async fn connect_with_sslmode(connection_string: &str) -> Result<tokio_postgres::Client> {
    let mode = SslMode::from_connection_string(connection_string);
    match make_tls_connector(mode)? {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(connection_string, tls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "postgres connection ended");
                }
            });
            Ok(client)
        }
        None => {
            let (client, connection) =
                tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "postgres connection ended");
                }
            });
            Ok(client)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_disable() {
        assert_eq!(SslMode::parse("disable"), SslMode::Disable);
        assert_eq!(SslMode::parse("DISABLE"), SslMode::Disable);
    }

    #[test]
    fn parse_verify_modes() {
        assert_eq!(SslMode::parse("verify-ca"), SslMode::VerifyFull);
        assert_eq!(SslMode::parse("verify-full"), SslMode::VerifyFull);
    }

    #[test]
    fn parse_unknown_falls_back_to_require() {
        assert_eq!(SslMode::parse(""), SslMode::Require);
        assert_eq!(SslMode::parse("prefer"), SslMode::Require);
        assert_eq!(SslMode::parse("require"), SslMode::Require);
        assert_eq!(SslMode::parse("nonsense"), SslMode::Require);
    }

    #[test]
    fn from_connection_string_no_query() {
        assert_eq!(
            SslMode::from_connection_string("postgresql://u@h/db"),
            SslMode::Require
        );
    }

    #[test]
    fn from_connection_string_picks_up_sslmode() {
        assert_eq!(
            SslMode::from_connection_string("postgresql://u@h/db?sslmode=disable"),
            SslMode::Disable
        );
        assert_eq!(
            SslMode::from_connection_string(
                "postgresql://u@h/db?application_name=x&sslmode=verify-full"
            ),
            SslMode::VerifyFull
        );
    }

    #[test]
    fn from_connection_string_missing_sslmode_defaults_require() {
        assert_eq!(
            SslMode::from_connection_string("postgresql://u@h/db?application_name=x"),
            SslMode::Require
        );
    }

    #[test]
    fn make_tls_connector_returns_none_for_disable() {
        let c = make_tls_connector(SslMode::Disable).unwrap();
        assert!(c.is_none());
    }

    #[test]
    fn make_tls_connector_returns_some_for_require() {
        let c = make_tls_connector(SslMode::Require).unwrap();
        assert!(c.is_some());
    }

    #[test]
    fn make_tls_connector_returns_some_for_verify_full() {
        let c = make_tls_connector(SslMode::VerifyFull).unwrap();
        assert!(c.is_some());
    }

    #[test]
    fn no_cert_verification_verify_server_cert_always_succeeds() {
        use rustls::client::danger::ServerCertVerifier;
        use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

        let verifier = NoCertVerification::default();
        let dummy_cert = CertificateDer::from(vec![0u8; 32]);
        let server_name = ServerName::try_from("example.com").unwrap();
        let result =
            verifier.verify_server_cert(&dummy_cert, &[], &server_name, &[], UnixTime::now());
        assert!(result.is_ok());
    }

    #[test]
    fn no_cert_verification_supported_verify_schemes_non_empty() {
        use rustls::client::danger::ServerCertVerifier;

        let verifier = NoCertVerification::default();
        let schemes = verifier.supported_verify_schemes();
        assert!(!schemes.is_empty());
    }

    #[test]
    fn ssl_mode_debug_and_clone() {
        let mode = SslMode::Require;
        let cloned = mode;
        assert_eq!(cloned, SslMode::Require);
        let dbg = format!("{:?}", mode);
        assert!(dbg.contains("Require"));
    }

    #[test]
    fn ssl_mode_parse_with_leading_trailing_whitespace() {
        assert_eq!(SslMode::parse("  disable  "), SslMode::Disable);
        assert_eq!(SslMode::parse(" verify-full "), SslMode::VerifyFull);
        assert_eq!(SslMode::parse(" REQUIRE "), SslMode::Require);
    }

    #[test]
    fn from_connection_string_sslmode_first_param() {
        assert_eq!(
            SslMode::from_connection_string("postgresql://u@h/db?sslmode=verify-ca&timeout=10"),
            SslMode::VerifyFull
        );
    }

    #[test]
    fn from_connection_string_sslmode_case_insensitive_in_value() {
        assert_eq!(
            SslMode::from_connection_string("postgresql://u@h/db?sslmode=DISABLE"),
            SslMode::Disable
        );
    }

    #[test]
    fn no_cert_verification_debug_format() {
        let v = NoCertVerification::default();
        let dbg = format!("{:?}", v);
        assert!(dbg.contains("NoCertVerification"));
    }

    #[test]
    fn make_tls_connector_require_and_verify_full_produce_different_configs() {
        let require = make_tls_connector(SslMode::Require).unwrap().unwrap();
        let verify = make_tls_connector(SslMode::VerifyFull).unwrap().unwrap();
        // Both produce connectors, they just differ in verification behavior.
        // We can't deeply inspect them, but we verify they are constructed.
        let _ = require;
        let _ = verify;
    }
}
