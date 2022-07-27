use std::io::{Seek, SeekFrom, Write};

pub struct VecWriter {
    pos: usize,
    pub buf: Vec<u8>,
}

impl VecWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() == self.pos {
            self.buf.extend(buf);
        } else {
            if self.pos + buf.len() <= self.buf.len() {
                self.buf[self.pos..self.pos + buf.len()].copy_from_slice(buf);
            } else {
                let copy_len = self.buf.len() - self.pos;

                self.buf[self.pos..self.pos + copy_len].copy_from_slice(&buf[..copy_len]);

                self.buf.extend(&buf[copy_len..]);
            }
        }

        self.pos += buf.len();

        return Ok(buf.len());
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for VecWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                self.pos = offset as usize;
            }
            SeekFrom::End(offset) => {
                self.pos = self.buf.len() + offset as usize;
            }
            SeekFrom::Current(offset) => {
                self.pos += offset as usize;
            }
        };

        Ok(self.pos as u64)
    }
}
