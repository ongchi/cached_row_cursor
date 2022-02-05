use std::collections::BTreeMap;
use std::io::{BufRead, Read, Seek, SeekFrom};

pub struct CachedRowCursor<T> {
    inner: T,
    pos: u64,
    row_pos: u64,
    length: Option<u64>,
    row_length: Option<u64>,
    separator: u8,
    granularity: u64,
    cached_index: BTreeMap<u64, u64>,
}

impl<T: BufRead + Seek> CachedRowCursor<T> {
    pub fn new(reader: T, separator: u8, granularity: u64) -> Self {
        Self {
            inner: reader,
            pos: 0,
            row_pos: 0,
            length: None,
            row_length: None,
            separator,
            granularity,
            cached_index: BTreeMap::from([(0, 0)]),
        }
    }

    // Return current byte position
    pub fn position(&self) -> u64 {
        self.pos
    }

    // Set byte position
    pub fn set_position(&mut self, pos: u64) -> Result<u64, std::io::Error> {
        let (&cached_row, &cached_byte) = self
            .cached_index
            .iter()
            .take_while(|(&_row, &byte)| byte < pos)
            .last()
            .unwrap_or((&0, &0));

        self.pos = self
            .inner
            .seek(SeekFrom::Current(cached_byte as i64 - self.pos as i64))?;
        self.row_pos = cached_row;

        let mut byte_len;
        let mut buf = vec![];
        while self.pos < pos {
            byte_len = self.read_row(&mut buf)?;
            if byte_len == 0 {
                break;
            }
        }

        if self.pos > pos {
            self.inner
                .seek(SeekFrom::Current(pos as i64 - self.pos as i64))?;
            self.pos = pos;
            self.row_pos -= 1;
        }

        Ok(self.pos)
    }

    pub fn row_position(&mut self) -> u64 {
        self.row_pos
    }

    pub fn set_row_position(&mut self, row: u64) -> Result<u64, std::io::Error> {
        // Get nearest cached position
        let (&cached_row, &cached_byte) = self
            .cached_index
            .get_key_value(&(row / self.granularity))
            .unwrap_or_else(|| self.cached_index.iter().last().unwrap_or((&0, &0)));

        self.inner
            .seek(SeekFrom::Current(cached_byte as i64 - self.pos as i64))?;
        self.row_pos = cached_row;
        self.pos = cached_byte;

        let mut buf = vec![];
        while self.row_pos < row && self.read_row(&mut buf).unwrap() != 0 {}

        Ok(self.row_pos)
    }

    pub fn read_row(&mut self, buf: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let byte_len = self.inner.read_until(self.separator, buf)?;

        if byte_len != 0 {
            self.pos += byte_len as u64;
            self.row_pos += 1;

            if self.row_pos % self.granularity == 0
                && !self.cached_index.contains_key(&self.row_pos)
            {
                self.cached_index.insert(self.row_pos, self.pos);
            }
        } else {
            // Reached EOF
            self.row_length = Some(self.row_pos);
            self.length = Some(self.pos);
        }

        Ok(byte_len)
    }

    pub fn seek_row(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        let pos = match pos {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::Current(pos) => self.row_pos as i64 + pos,
            SeekFrom::End(pos) => {
                let mut buf = vec![];
                while self.row_length.is_none() {
                    self.read_row(&mut buf)?;
                }
                self.row_length.unwrap() as i64 - 1 + pos
            }
        };

        if pos < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative row position",
            ))
        } else {
            self.set_row_position(pos as u64)
        }
    }
}

impl<T: Read> Read for CachedRowCursor<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;
        self.row_pos += buf.iter().filter(|&&s| s == self.separator).count() as u64;
        Ok(n)
    }
}

impl<T: BufRead> BufRead for CachedRowCursor<T> {
    fn fill_buf(&mut self) -> Result<&[u8], std::io::Error> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.pos += amt as u64;
        self.inner.consume(amt)
    }

    fn read_until(&mut self, byte: u8, buf: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let result = self.inner.read_until(byte, buf);
        self.row_pos += if byte != self.separator {
            buf.iter()
                .take_while(|&&s| s != byte)
                .filter(|&&s| s == self.separator)
                .count() as u64
        } else {
            1
        };
        result
    }
}

impl<T: BufRead + Seek> Seek for CachedRowCursor<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        let pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(n) => self.pos as i64 + n,
            SeekFrom::End(n) => {
                let mut buf = vec![];
                while self.length.is_none() {
                    self.read_row(&mut buf)?;
                }
                self.length.unwrap() as i64 - 1 + n
            }
        };

        if pos < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            ))
        } else {
            self.set_position(pos as u64)
        }
    }

    fn stream_position(&mut self) -> Result<u64, std::io::Error> {
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::CachedRowCursor;
    use std::io::{BufReader, Cursor, Seek, SeekFrom};

    fn make_cursor() -> CachedRowCursor<BufReader<Cursor<&'static [u8; 20]>>> {
        let data = BufReader::new(Cursor::new(b"foo\nbar\nbiz\nbaz\nbuz\n"));
        CachedRowCursor::new(data, b'\n', 1)
    }

    #[test]
    fn position() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.set_position(12).unwrap(), 12);
        assert_eq!(cursor.position(), 12);
        assert_eq!(cursor.row_position(), 3);
    }

    #[test]
    fn row_position() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.set_row_position(2).unwrap(), 2);
        assert_eq!(cursor.row_position(), 2);
        assert_eq!(cursor.position(), 8);
    }

    #[test]
    fn seek_from_start() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek(SeekFrom::Start(1)).unwrap(), 1);
        assert_eq!(cursor.position(), 1);
        assert_eq!(cursor.row_position(), 0);

        assert_eq!(cursor.seek(SeekFrom::Start(4)).unwrap(), 4);
        assert_eq!(cursor.position(), 4);
        assert_eq!(cursor.row_position(), 1);

        assert_eq!(cursor.seek(SeekFrom::Start(21)).unwrap(), 20);
        assert_eq!(cursor.position(), 20);
        assert_eq!(cursor.row_position(), 5);
    }

    #[test]
    fn seek_from_current() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek(SeekFrom::Current(2)).unwrap(), 2);
        assert_eq!(cursor.position(), 2);
        assert_eq!(cursor.row_position(), 0);

        assert_eq!(cursor.seek(SeekFrom::Current(2)).unwrap(), 4);
        assert_eq!(cursor.position(), 4);
        assert_eq!(cursor.row_position(), 1);

        assert!(cursor.seek(SeekFrom::Current(-5)).is_err());
    }

    #[test]
    fn seek_from_end() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek(SeekFrom::End(-7)).unwrap(), 12);
        assert_eq!(cursor.position(), 12);
        assert_eq!(cursor.row_position(), 3);

        assert_eq!(cursor.seek(SeekFrom::End(-19)).unwrap(), 0);
        assert_eq!(cursor.position(), 0);
        assert_eq!(cursor.row_position(), 0);

        assert!(cursor.seek(SeekFrom::End(-20)).is_err());

        assert_eq!(cursor.length.unwrap(), 20);
    }

    #[test]
    fn seek_row_from_start() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek_row(SeekFrom::Start(3)).unwrap(), 3);
        assert_eq!(cursor.row_position(), 3);
        assert_eq!(cursor.position(), 12);

        assert_eq!(cursor.seek_row(SeekFrom::Start(6)).unwrap(), 5);
        assert_eq!(cursor.row_position(), 5);
        assert_eq!(cursor.position(), 20);
    }

    #[test]
    fn seek_row_from_current() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek_row(SeekFrom::Current(1)).unwrap(), 1);
        assert_eq!(cursor.row_position(), 1);
        assert_eq!(cursor.position(), 4);

        assert_eq!(cursor.seek_row(SeekFrom::Current(2)).unwrap(), 3);
        assert_eq!(cursor.row_position(), 3);
        assert_eq!(cursor.position(), 12);

        assert!(cursor.seek_row(SeekFrom::Current(-4)).is_err());
    }

    #[test]
    fn seek_row_from_end() {
        let mut cursor = make_cursor();

        assert_eq!(cursor.seek_row(SeekFrom::End(-1)).unwrap(), 3);
        assert_eq!(cursor.row_position(), 3);
        assert_eq!(cursor.position(), 12);

        assert!(cursor.seek_row(SeekFrom::End(-5)).is_err());

        assert_eq!(cursor.row_length.unwrap(), 5);
    }

    #[test]
    fn read_row() {
        let mut cursor = make_cursor();

        let mut buf = vec![];
        assert_eq!(cursor.read_row(&mut buf).unwrap(), 4);
        assert_eq!(buf, "foo\n".as_bytes());
        assert_eq!(cursor.position(), 4);
        assert_eq!(cursor.row_position(), 1);

        assert_eq!(cursor.seek(SeekFrom::Current(1)).unwrap(), 5);
        let mut buf = vec![];
        assert_eq!(cursor.read_row(&mut buf).unwrap(), 3);
        assert_eq!(buf, b"ar\n");
        assert_eq!(cursor.position(), 8);
        assert_eq!(cursor.row_position(), 2);

        assert_eq!(cursor.seek(SeekFrom::End(0)).unwrap(), 19);
        let mut buf = vec![];
        assert_eq!(cursor.read_row(&mut buf).unwrap(), 1);
        assert_eq!(buf, b"\n");
        assert_eq!(cursor.position(), 20);
        assert_eq!(cursor.row_position(), 5);
    }

    #[test]
    fn separator() {
        let mut cursor = make_cursor();
        cursor.separator = b'a';

        assert_eq!(cursor.seek(SeekFrom::End(0)).unwrap(), 19);
        assert_eq!(cursor.row_position(), 2);

        assert_eq!(cursor.set_row_position(1).unwrap(), 1);
        let mut buf = vec![];
        assert_eq!(cursor.read_row(&mut buf).unwrap(), 8);
        assert_eq!(buf, b"r\nbiz\nba");
        assert_eq!(cursor.position(), 14);
        assert_eq!(cursor.row_position(), 2);
    }

    #[test]
    fn granularity() {
        let mut cursor = make_cursor();
        cursor.granularity = 2;

        assert_eq!(cursor.seek(SeekFrom::End(0)).unwrap(), 19);
        assert_eq!(cursor.cached_index.len(), 3);
    }
}
