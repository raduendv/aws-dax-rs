//! DAX tube initialization and authorization framing.

use std::time::Duration;

use aws_credential_types::Credentials;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use super::{
    cbor::{write_bytes, write_i64, write_text, write_type},
    stream::{StreamCborError, read_cbor_item, read_response_envelope, read_scan_response_body},
};

const AUTHORIZATION_METHOD_ID: i64 = 1_489_122_155;
const TUBE_MAGIC: &str = "J7yne5G";
const TUBE_USER_AGENT: &str = "DaxGoV2Client-1.0.3";
const AUTHORIZATION_USER_AGENT: &str = "DaxGoClient-1.0.0";
const AUTHORIZATION_WINDOW: Duration = Duration::from_secs(225);
const MAJOR_MAP: u8 = 0xa0;

/// State that a pooled DAX tube retains between operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TubeAuthentication {
    access_key_id: String,
    expires_at: OffsetDateTime,
}

impl TubeAuthentication {
    /// Returns whether the tube needs authorization for these credentials.
    pub(crate) fn is_current(&self, credentials: &Credentials, now: OffsetDateTime) -> bool {
        self.access_key_id == credentials.access_key_id() && now < self.expires_at
    }

    /// Records the completed authorization's validity window.
    pub(crate) fn new(credentials: &Credentials, now: OffsetDateTime) -> Self {
        Self {
            access_key_id: credentials.access_key_id().into(),
            expires_at: now + AUTHORIZATION_WINDOW,
        }
    }

    /// Expires this state after an authentication-required server failure.
    pub(crate) fn expire(&mut self, now: OffsetDateTime) {
        self.expires_at = now;
    }
}

/// One checked-out DAX tube with independent asynchronous read and write halves.
///
/// This low-level type drains one complete DAX response before a tube is reused.
#[derive(Debug)]
pub(crate) struct ControlTube<Reader, Writer> {
    reader: Reader,
    writer: Writer,
}

impl<Stream> ControlTube<ReadHalf<Stream>, WriteHalf<Stream>>
where
    Stream: AsyncRead + AsyncWrite + Unpin,
{
    /// Initializes a connected tube and flushes its required DAX preamble.
    pub(crate) async fn from_stream(stream: Stream) -> Result<Self, StreamCborError> {
        let (reader, mut writer) = tokio::io::split(stream);
        writer
            .write_all(&encode_tube_preamble())
            .await
            .map_err(StreamCborError::Io)?;
        writer.flush().await.map_err(StreamCborError::Io)?;
        Ok(Self { reader, writer })
    }
}

impl<Reader, Writer> ControlTube<Reader, Writer>
where
    Reader: AsyncRead + Unpin + Send,
    Writer: AsyncWrite + Unpin + Send,
{
    /// Writes and flushes fresh DAX authorization without reading a response.
    pub(crate) async fn authorize(
        &mut self,
        credentials: &Credentials,
        region: &str,
        now: OffsetDateTime,
    ) -> Result<(), TubeError> {
        let authorization = encode_authorization(credentials, region, now)?;
        self.writer
            .write_all(&authorization)
            .await
            .map_err(|_| TubeError::WriteFailed)?;
        self.writer
            .flush()
            .await
            .map_err(|_| TubeError::WriteFailed)
    }

    /// Writes one control request and drains its complete DAX response.
    pub(crate) async fn execute(
        &mut self,
        operation: &'static str,
        request: &[u8],
    ) -> Result<Vec<u8>, StreamCborError> {
        self.writer
            .write_all(request)
            .await
            .map_err(StreamCborError::Io)?;
        self.writer.flush().await.map_err(StreamCborError::Io)?;

        let (mut envelope, success) = read_response_envelope(&mut self.reader).await?;
        if success {
            if matches!(operation, "Scan" | "Query") {
                envelope.extend(read_scan_response_body(&mut self.reader).await?);
            } else {
                envelope.extend(read_cbor_item(&mut self.reader).await?);
            }
        }
        Ok(envelope)
    }
}

/// Encodes the exact DAX tube preamble written once after connecting.
pub(crate) fn encode_tube_preamble() -> Vec<u8> {
    let mut output = Vec::new();
    write_text(&mut output, TUBE_MAGIC);
    write_i64(&mut output, 0);
    write_text(&mut output, "0");
    write_type(&mut output, MAJOR_MAP, 1);
    write_text(&mut output, "UserAgent");
    write_text(&mut output, TUBE_USER_AGENT);
    write_i64(&mut output, 0);
    output
}

/// Generates and frames DAX connection authorization for one tube.
pub(crate) fn encode_authorization(
    credentials: &Credentials,
    region: &str,
    now: OffsetDateTime,
) -> Result<Vec<u8>, TubeError> {
    let timestamp = format_amz_timestamp(now)?;
    let date = &timestamp[..8];
    let canonical_request = format!(
        "POST\n/\n\nhost:https://dax.amazonaws.com\nx-amz-date:{timestamp}\n\nhost;x-amz-date\n{}",
        hex(&Sha256::digest([]))
    );
    let scope = format!("{date}/{region}/dax/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signature = sign(
        credentials.secret_access_key(),
        date,
        region,
        &string_to_sign,
    )?;

    let mut output = Vec::new();
    write_i64(&mut output, 1);
    write_i64(&mut output, AUTHORIZATION_METHOD_ID);
    write_text(&mut output, credentials.access_key_id());
    write_text(&mut output, &signature);
    write_bytes(&mut output, string_to_sign.as_bytes());
    match credentials.session_token() {
        Some(token) => write_text(&mut output, token),
        None => output.push(0xf6),
    }
    write_text(&mut output, AUTHORIZATION_USER_AGENT);
    Ok(output)
}

/// Failures while preparing a DAX authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TubeError {
    /// The authorization timestamp could not be represented in the DAX format.
    InvalidTimestamp,
    /// HMAC key initialization failed.
    InvalidSigningKey,
    /// Authorization bytes could not be written to the tube.
    WriteFailed,
}

impl std::fmt::Display for TubeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTimestamp => write!(formatter, "invalid DAX authorization timestamp"),
            Self::InvalidSigningKey => write!(formatter, "invalid DAX authorization signing key"),
            Self::WriteFailed => write!(formatter, "failed to write DAX authorization"),
        }
    }
}

fn format_amz_timestamp(now: OffsetDateTime) -> Result<String, TubeError> {
    now.format(&time::macros::format_description!(
        "[year][month][day]T[hour][minute][second]Z"
    ))
    .map_err(|_| TubeError::InvalidTimestamp)
}

fn sign(secret: &str, date: &str, region: &str, string_to_sign: &str) -> Result<String, TubeError> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, b"dax")?;
    let signing_key = hmac(&service_key, b"aws4_request")?;
    Ok(hex(&hmac(&signing_key, string_to_sign.as_bytes())?))
}

fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, TubeError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| TubeError::InvalidSigningKey)?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use aws_credential_types::Credentials;
    use time::{Date, Month, Time};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::{ControlTube, TubeAuthentication, encode_authorization, encode_tube_preamble};

    fn timestamp() -> time::OffsetDateTime {
        Date::from_calendar_date(2015, Month::August, 30)
            .expect("fixed date is valid")
            .with_time(Time::from_hms(12, 36, 0).expect("fixed time is valid"))
            .assume_utc()
    }

    #[test]
    fn encodes_the_reference_tube_preamble() {
        assert_eq!(
            encode_tube_preamble(),
            [
                0x67, b'J', b'7', b'y', b'n', b'e', b'5', b'G', 0x00, 0x61, b'0', 0xa1, 0x69, b'U',
                b's', b'e', b'r', b'A', b'g', b'e', b'n', b't', 0x73, b'D', b'a', b'x', b'G', b'o',
                b'V', b'2', b'C', b'l', b'i', b'e', b'n', b't', b'-', b'1', b'.', b'0', b'.', b'3',
                0x00,
            ]
        );
    }

    #[test]
    fn authorization_signing_uses_the_dax_canonical_request() {
        let credentials = Credentials::new("AKIDEXAMPLE", "SECRET", None, None, "test");
        let frame = encode_authorization(&credentials, "us-east-1", timestamp()).unwrap();
        let signature = b"d15b4e9b05573e403a3f8b53f66d5a2f2b2a1e626553d22339c8f9e1f3a95995";
        let string_to_sign = b"AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/dax/aws4_request\n8f75d5e14aa320ae32afd82c63ddf96bb8673dae15f9271cacd7a035576507b9";
        let mut expected = vec![0x01, 0x1a, 0x58, 0xc2, 0x33, 0x6b, 0x6b];
        expected.extend_from_slice(b"AKIDEXAMPLE");
        expected.extend_from_slice(&[0x78, 0x40]);
        expected.extend_from_slice(signature);
        expected.extend_from_slice(&[0x58, 0x86]);
        expected.extend_from_slice(string_to_sign);
        expected.extend_from_slice(&[0xf6, 0x71]);
        expected.extend_from_slice(b"DaxGoClient-1.0.0");
        assert_eq!(frame, expected);

        let session_credentials = Credentials::new(
            "AKIDEXAMPLE",
            "SECRET",
            Some("session-token".into()),
            None,
            "test",
        );
        let session_frame =
            encode_authorization(&session_credentials, "us-east-1", timestamp()).unwrap();
        assert!(
            session_frame
                .windows(signature.len())
                .any(|value| value == signature)
        );
        assert!(session_frame.ends_with(b"\x6dsession-token\x71DaxGoClient-1.0.0"));
    }

    #[test]
    fn authorization_state_refreshes_at_the_reference_window() {
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let now = timestamp();
        let mut authentication = TubeAuthentication::new(&credentials, now);
        assert!(authentication.is_current(&credentials, now));
        assert!(authentication.is_current(&credentials, now + time::Duration::seconds(224)));
        assert!(!authentication.is_current(&credentials, now + time::Duration::seconds(225)));
        authentication.expire(now);
        assert!(!authentication.is_current(&credentials, now));
    }

    #[tokio::test]
    async fn drains_one_complete_control_response_before_reuse() {
        let (client, mut server) = duplex(512);
        let server_task = tokio::spawn(async move {
            let mut preamble = vec![0; encode_tube_preamble().len()];
            server.read_exact(&mut preamble).await.unwrap();
            assert_eq!(preamble, encode_tube_preamble());

            let mut request = [0; 2];
            server.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0x01, 0x02]);
            server.write_all(&[0x80, 0x09]).await.unwrap();

            let mut next_request = [0; 1];
            server.read_exact(&mut next_request).await.unwrap();
            assert_eq!(next_request, [0x03]);
            server
                .write_all(&[0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6])
                .await
                .unwrap();
        });

        let mut tube = ControlTube::from_stream(client).await.unwrap();
        assert_eq!(
            tube.execute("Test", &[0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        assert_eq!(
            tube.execute("Test", &[0x03]).await.unwrap(),
            vec![0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6]
        );
        server_task.await.unwrap();
    }
}
