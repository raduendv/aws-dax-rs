//! Bounded stream framing for self-delimiting DAX CBOR values.

use std::{future::Future, pin::Pin};

use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONTAINER_ELEMENTS: usize = 100_000;
const MAX_NESTING_DEPTH: usize = 64;

/// A failure while reading one complete CBOR item from a DAX stream.
#[derive(Debug)]
pub(crate) enum StreamCborError {
    /// Socket input ended or otherwise failed while a value was being read.
    Io(std::io::Error),
    /// A value exceeded the fixed protocol input bound.
    ValueTooLarge,
    /// A container declared too many values.
    ContainerTooLarge,
    /// A value was nested beyond the fixed protocol input bound.
    NestingTooDeep,
    /// The stream contained a CBOR form that cannot delimit one value safely.
    InvalidForm(&'static str),
}

impl std::fmt::Display for StreamCborError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "CBOR stream I/O failed: {error}"),
            Self::ValueTooLarge => write!(formatter, "CBOR stream value exceeds size limit"),
            Self::ContainerTooLarge => {
                write!(formatter, "CBOR stream container exceeds element limit")
            }
            Self::NestingTooDeep => write!(formatter, "CBOR stream nesting exceeds depth limit"),
            Self::InvalidForm(form) => write!(formatter, "invalid CBOR stream form: {form}"),
        }
    }
}

/// Reads exactly one complete self-delimiting CBOR item.
///
/// Any following CBOR values remain unread, which lets a tube decode an error
/// envelope and then precisely one operation response body before reuse.
pub(crate) async fn read_cbor_item<Reader>(reader: &mut Reader) -> Result<Vec<u8>, StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let mut output = Vec::new();
    read_item(reader, &mut output, 0).await?;
    Ok(output)
}

/// Reads the DAX response envelope, which is a CBOR error-code array followed
/// by message and detail items only for failures.
pub(crate) async fn read_response_envelope<Reader>(
    reader: &mut Reader,
) -> Result<(Vec<u8>, bool), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let mut envelope = read_cbor_item(reader).await?;
    if envelope == [0x80] {
        return Ok((envelope, true));
    }
    envelope.extend(read_cbor_item(reader).await?);
    envelope.extend(read_cbor_item(reader).await?);
    Ok((envelope, false))
}

/// Reads a Scan response body, including DAX's non-canonical consumed-capacity
/// payload that follows response ordinal `1` as multiple CBOR values.
pub(crate) async fn read_scan_response_body<Reader>(
    reader: &mut Reader,
) -> Result<Vec<u8>, StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let mut output = Vec::new();
    let initial = read_byte(reader, &mut output).await?;
    if initial & 0xe0 != 0xa0 {
        return Err(StreamCborError::InvalidForm("Scan response is not a map"));
    }
    let length = read_length(reader, &mut output, initial & 0x1f).await?;
    if let Some(length) = length {
        if length > MAX_CONTAINER_ELEMENTS {
            return Err(StreamCborError::ContainerTooLarge);
        }
        for _ in 0..length {
            read_scan_response_entry(reader, &mut output).await?;
        }
        return Ok(output);
    }

    let mut entries = 0_usize;
    loop {
        let initial = read_byte(reader, &mut output).await?;
        if initial == 0xff {
            return Ok(output);
        }
        entries = entries.saturating_add(1);
        if entries > MAX_CONTAINER_ELEMENTS {
            return Err(StreamCborError::ContainerTooLarge);
        }
        let key_start = output.len() - 1;
        read_item_initial(reader, &mut output, 1, initial).await?;
        let key = output[key_start..].to_vec();
        read_scan_response_value(reader, &mut output, &key).await?;
    }
}

async fn read_scan_response_entry<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let key_start = output.len();
    read_item(reader, output, 1).await?;
    let key = output[key_start..].to_vec();
    read_scan_response_value(reader, output, &key).await
}

async fn read_scan_response_value<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    key: &[u8],
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    if key != [0x01] {
        return read_item(reader, output, 1).await;
    }

    let capacity = read_cbor_item(reader).await?;
    let absent = capacity == [0xf6];
    output.extend(capacity);
    if absent {
        return Ok(());
    }
    read_cbor_item(reader)
        .await
        .map(|table| output.extend(table))?;
    read_cbor_item(reader)
        .await
        .map(|units| output.extend(units))?;
    for _ in 0..3 {
        read_cbor_item(reader)
            .await
            .map(|component| output.extend(component))?;
    }
    Ok(())
}

type BoxFuture<'a> = Pin<Box<dyn Future<Output = Result<(), StreamCborError>> + Send + 'a>>;

fn read_item<'a, Reader>(
    reader: &'a mut Reader,
    output: &'a mut Vec<u8>,
    depth: usize,
) -> BoxFuture<'a>
where
    Reader: AsyncRead + Unpin + Send + 'a,
{
    Box::pin(async move {
        if depth > MAX_NESTING_DEPTH {
            return Err(StreamCborError::NestingTooDeep);
        }
        let initial = read_byte(reader, output).await?;
        read_item_initial(reader, output, depth, initial).await
    })
}

fn read_item_initial<'a, Reader>(
    reader: &'a mut Reader,
    output: &'a mut Vec<u8>,
    depth: usize,
    initial: u8,
) -> BoxFuture<'a>
where
    Reader: AsyncRead + Unpin + Send + 'a,
{
    Box::pin(async move {
        if initial == 0xff {
            return Err(StreamCborError::InvalidForm("unexpected break"));
        }
        let major = initial & 0xe0;
        let additional = initial & 0x1f;
        if major == 0xe0 {
            return match additional {
                0..=23 => Ok(()),
                24 => read_payload(reader, output, 1).await,
                25 => read_payload(reader, output, 2).await,
                26 => read_payload(reader, output, 4).await,
                27 => read_payload(reader, output, 8).await,
                28..=30 => Err(StreamCborError::InvalidForm("reserved simple value")),
                31 => Err(StreamCborError::InvalidForm("unexpected break")),
                _ => unreachable!(),
            };
        }
        let length = read_length(reader, output, additional).await?;

        match major {
            0x00 | 0x20 => require_definite(length, "indefinite integer"),
            0x40 | 0x60 => read_string(reader, output, major, length).await,
            0x80 => read_container(reader, output, length, depth, 1).await,
            0xa0 => read_container(reader, output, length, depth, 2).await,
            0xc0 => {
                require_definite(length, "indefinite tag")?;
                read_item(reader, output, depth + 1).await
            }
            _ => Err(StreamCborError::InvalidForm("unknown major type")),
        }
    })
}

async fn read_string<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    major: u8,
    length: Option<usize>,
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    if let Some(length) = length {
        read_payload(reader, output, length).await?;
        return Ok(());
    }

    loop {
        let initial = read_byte(reader, output).await?;
        if initial == 0xff {
            return Ok(());
        }
        if initial & 0xe0 != major {
            return Err(StreamCborError::InvalidForm(
                "indefinite string contains a different item type",
            ));
        }
        let length = read_length(reader, output, initial & 0x1f).await?;
        let length = length.ok_or(StreamCborError::InvalidForm(
            "indefinite string contains an indefinite chunk",
        ))?;
        read_payload(reader, output, length).await?;
    }
}

async fn read_container<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    length: Option<usize>,
    depth: usize,
    values_per_element: usize,
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    if let Some(length) = length {
        if length > MAX_CONTAINER_ELEMENTS {
            return Err(StreamCborError::ContainerTooLarge);
        }
        for _ in 0..length.saturating_mul(values_per_element) {
            read_item(reader, output, depth + 1).await?;
        }
        return Ok(());
    }

    let mut values = 0_usize;
    loop {
        let initial = read_byte(reader, output).await?;
        if initial == 0xff {
            return Ok(());
        }
        values = values.saturating_add(1);
        if values > MAX_CONTAINER_ELEMENTS.saturating_mul(values_per_element) {
            return Err(StreamCborError::ContainerTooLarge);
        }
        read_item_initial(reader, output, depth + 1, initial).await?;
    }
}

fn require_definite(length: Option<usize>, form: &'static str) -> Result<(), StreamCborError> {
    length
        .is_some()
        .then_some(())
        .ok_or(StreamCborError::InvalidForm(form))
}

async fn read_length<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    additional: u8,
) -> Result<Option<usize>, StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let bytes = match additional {
        value @ 0..=23 => return Ok(Some(usize::from(value))),
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        31 => return Ok(None),
        _ => {
            return Err(StreamCborError::InvalidForm(
                "reserved additional information",
            ));
        }
    };
    let mut encoded = [0_u8; 8];
    read_exact(reader, output, &mut encoded[8 - bytes..]).await?;
    let value = u64::from_be_bytes(encoded);
    usize::try_from(value)
        .map_err(|_| StreamCborError::ValueTooLarge)
        .map(Some)
}

async fn read_payload<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    length: usize,
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    if length > MAX_VALUE_BYTES || output.len().saturating_add(length) > MAX_VALUE_BYTES {
        return Err(StreamCborError::ValueTooLarge);
    }
    let start = output.len();
    output.resize(start + length, 0);
    reader
        .read_exact(&mut output[start..])
        .await
        .map_err(StreamCborError::Io)?;
    Ok(())
}

async fn read_byte<Reader>(reader: &mut Reader, output: &mut Vec<u8>) -> Result<u8, StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    let mut byte = [0_u8; 1];
    read_exact(reader, output, &mut byte).await?;
    Ok(byte[0])
}

async fn read_exact<Reader>(
    reader: &mut Reader,
    output: &mut Vec<u8>,
    bytes: &mut [u8],
) -> Result<(), StreamCborError>
where
    Reader: AsyncRead + Unpin + Send,
{
    if output.len().saturating_add(bytes.len()) > MAX_VALUE_BYTES {
        return Err(StreamCborError::ValueTooLarge);
    }
    reader
        .read_exact(bytes)
        .await
        .map_err(StreamCborError::Io)?;
    output.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};

    use super::{StreamCborError, read_cbor_item, read_response_envelope, read_scan_response_body};

    #[tokio::test]
    async fn preserves_exact_values_and_leaves_the_following_value_unread() {
        let (mut writer, mut reader) = duplex(64);
        writer
            .write_all(&[
                0x80, // DAX success envelope
                0xbf, // indefinite response map
                0x00, 0x58, 0x18, // 24-byte value
                b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd',
                b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n', 0xff, // end map
                0x09, // following value
            ])
            .await
            .unwrap();

        assert_eq!(read_cbor_item(&mut reader).await.unwrap(), vec![0x80]);
        assert_eq!(
            read_cbor_item(&mut reader).await.unwrap(),
            [
                0xbf, 0x00, 0x58, 0x18, b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9',
                b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n',
                0xff,
            ]
        );
        assert_eq!(read_cbor_item(&mut reader).await.unwrap(), vec![0x09]);
    }

    #[tokio::test]
    async fn preserves_floating_point_values_without_interpreting_bits_as_lengths() {
        let (mut writer, mut reader) = duplex(64);
        writer
            .write_all(&[
                0xfb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1.5
                0x09, // following value
            ])
            .await
            .unwrap();

        assert_eq!(
            read_cbor_item(&mut reader).await.unwrap(),
            vec![0xfb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(read_cbor_item(&mut reader).await.unwrap(), vec![0x09]);
    }

    #[tokio::test]
    async fn accepts_large_unsigned_integer_values() {
        let (mut writer, mut reader) = duplex(16);
        writer
            .write_all(&[0x1a, 0x12, 0x56, 0x91, 0xbc])
            .await
            .unwrap();

        assert_eq!(
            read_cbor_item(&mut reader).await.unwrap(),
            [0x1a, 0x12, 0x56, 0x91, 0xbc]
        );
    }

    #[tokio::test]
    async fn frames_scan_capacity_response_with_floating_point_values() {
        let body = vec![
            0xa4, 0x01, 0x40, 0x65, b'T', b'a', b'b', b'l', b'e', 0xfb, 0x3f, 0xf8, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xf6, 0xf6, 0xf6, 0x07, 0x81, 0x82, 0x41, b'v', 0x45, 0x09,
            0x63, b'o', b'l', b'd', 0x08, 0x01, 0x09, 0x41, b'w',
        ];
        let (mut writer, mut reader) = duplex(128);
        writer.write_all(&body).await.unwrap();

        assert_eq!(read_scan_response_body(&mut reader).await.unwrap(), body);
    }

    #[tokio::test]
    async fn rejects_unbounded_or_malformed_stream_forms() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&[0x5f, 0x5f]).await.unwrap();
        assert!(matches!(
            read_cbor_item(&mut reader).await,
            Err(StreamCborError::InvalidForm(
                "indefinite string contains an indefinite chunk"
            ))
        ));

        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&[0x9f, 0xff]).await.unwrap();
        assert_eq!(read_cbor_item(&mut reader).await.unwrap(), vec![0x9f, 0xff]);
    }

    #[tokio::test]
    async fn reads_composite_dax_error_envelopes_without_consuming_the_next_response() {
        let (mut writer, mut reader) = duplex(64);
        writer
            .write_all(&[
                0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6, // error envelope
                0x80, // next success envelope
            ])
            .await
            .unwrap();

        assert_eq!(
            read_response_envelope(&mut reader).await.unwrap(),
            (vec![0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6], false)
        );
        assert_eq!(
            read_response_envelope(&mut reader).await.unwrap(),
            (vec![0x80], true)
        );
    }
}
