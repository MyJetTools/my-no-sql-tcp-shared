use crate::vec_writer::VecWriter;
use std::io::{Cursor, Read, Write};

pub fn compress(payload: &[u8]) -> Result<Vec<u8>, zip::result::ZipError> {
    let mut writer = VecWriter::new();

    {
        let mut zip = zip::ZipWriter::new(&mut writer);

        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("d", options)?;

        let mut pos = 0;
        while pos < payload.len() {
            let size = zip.write(&payload[pos..])?;

            pos += size;
        }

        zip.finish()?;
    }

    Ok(writer.buf)
}

pub fn decompress(payload: &[u8]) -> Result<Vec<u8>, zip::result::ZipError> {
    let c = Cursor::new(payload.to_vec());

    let mut zip = zip::ZipArchive::new(c)?;

    let mut page_buffer: Vec<u8> = Vec::new();

    for i in 0..zip.len() {
        let mut zip_file = zip.by_index(i)?;

        if zip_file.name() == "d" {
            let mut buffer = [0u8; 1024 * 1024];

            loop {
                let read_size = zip_file.read(&mut buffer[..])?;
                if read_size == 0 {
                    break;
                }

                page_buffer.extend(&buffer[..read_size]);
            }
        }
    }

    Ok(page_buffer)
}
