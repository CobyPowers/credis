use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::{BufReader, BufWriter, Read, Write},
};

const MAX_READ_SIZE: usize = 1024;

#[derive(Debug, PartialEq)]
pub enum RespKind {
    SimpleString(String),
    BulkString(String),
    VerbatimString(String, String),
    Integer(i64),
    Double(f64),
    BigNumber(String),
    SimpleError(String),
    BulkError(String),
    Array(Vec<RespKind>),
    Push(Vec<RespKind>),
    // TODO: Implement a proper set instead of relying on vec
    Set(Vec<RespKind>),
    Map(HashMap<String, RespKind>),
    Attributes(HashMap<String, RespKind>),
    Null,
}

impl RespKind {
    fn encode(&self) -> String {
        match self {
            Self::SimpleString(val) => format!("+{}\r\n", val),
            RespKind::BulkString(val) => format!("${}\r\n{}\r\n", val.len(), val),
            RespKind::VerbatimString(encoding, val) => format!(
                "={}\r\n{}:{}\r\n",
                encoding.len() + val.len() + 1,
                encoding,
                val,
            ),
            RespKind::Integer(val) => format!(":{:+}\r\n", val),
            RespKind::Double(val) => format!(",{:.2e}\r\n", val),
            RespKind::BigNumber(val) => format!("({}\r\n", val),
            RespKind::SimpleError(val) => format!("-{}\r\n", val),
            RespKind::BulkError(val) => format!("!{}\r\n{}\r\n", val.len(), val),
            // TODO: Find a better way to consolidate encode matches
            RespKind::Array(arr) => {
                let vals: Vec<String> = arr.iter().map(|x| x.encode()).collect();
                format!("*{}\r\n{}\r\n", arr.len(), vals.join(""))
            }
            RespKind::Push(arr) => {
                let vals: Vec<String> = arr.iter().map(|x| x.encode()).collect();
                format!(">{}\r\n{}\r\n", arr.len(), vals.join(""))
            }
            RespKind::Set(arr) => {
                let vals: Vec<String> = arr.iter().map(|x| x.encode()).collect();
                format!("~{}\r\n{}\r\n", arr.len(), vals.join(""))
            }
            RespKind::Map(map) => {
                let vals: Vec<String> = map.iter().map(|(k, v)| k.clone() + &v.encode()).collect();
                format!("*{}\r\n{}\r\n", vals.len(), vals.join(""))
            }
            RespKind::Attributes(map) => {
                let vals: Vec<String> = map.iter().map(|(k, v)| k.clone() + &v.encode()).collect();
                format!("|{}\r\n{}\r\n", vals.len(), vals.join(""))
            }
            RespKind::Null => "_\r\n".to_string(),
        }
    }
}

impl Display for RespKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.encode().as_str())
    }
}

pub trait ToRespValue {
    fn to_resp_value(&self) -> RespKind;
}

impl ToRespValue for i64 {
    fn to_resp_value(&self) -> RespKind {
        RespKind::Integer(*self)
    }
}

impl ToRespValue for f64 {
    fn to_resp_value(&self) -> RespKind {
        RespKind::Double(*self)
    }
}

impl ToRespValue for &str {
    fn to_resp_value(&self) -> RespKind {
        RespKind::BulkString(self.to_string())
    }
}

impl ToRespValue for String {
    fn to_resp_value(&self) -> RespKind {
        RespKind::BulkString(self.clone())
    }
}

impl<T: ToRespValue> ToRespValue for Vec<T> {
    fn to_resp_value(&self) -> RespKind {
        RespKind::Array(self.iter().map(|x| x.to_resp_value()).collect())
    }
}

impl<T: ToRespValue> ToRespValue for HashMap<String, T> {
    fn to_resp_value(&self) -> RespKind {
        RespKind::Map(
            self.iter()
                .map(|(k, v)| (k.clone(), v.to_resp_value()))
                .collect(),
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RespError {
    DecodeError,
    EncodeError,
    InvalidType,
    InvalidKey,
    InvalidStr,
    InvalidLength,
    InvalidInt,
    InvalidDouble,
    MismatchedLength,
    MissingTerminator,
}

pub struct RespParser<R, W>
where
    R: Read,
    W: Write,
{
    reader: BufReader<R>,
    writer: BufWriter<W>,
}

impl<R, W> RespParser<R, W>
where
    R: Read,
    W: Write,
{
    pub fn new(reader: BufReader<R>, writer: BufWriter<W>) -> Self {
        Self { reader, writer }
    }

    pub fn decode(&mut self) -> Result<RespKind, RespError> {
        let mut buf = [0u8; MAX_READ_SIZE];
        let bytes_read = self
            .reader
            .read(&mut buf)
            .map_err(|_| RespError::DecodeError)?;

        let mut data = str::from_utf8(&buf[..bytes_read]).map_err(|_| RespError::DecodeError)?;
        Self::parse_data(&mut data)
    }

    pub fn encode(&mut self, data: RespKind) -> Result<(), RespError> {
        self.writer
            .write_all(data.encode().as_bytes())
            .map_err(|_| RespError::EncodeError)?;
        self.writer.flush().map_err(|_| RespError::EncodeError)?;
        Ok(())
    }

    fn parse_data(data: &mut &str) -> Result<RespKind, RespError> {
        match Self::consume_type(data)? {
            '+' => {
                // Simple string
                let value = Self::consume_terminated_str(data)?;
                Ok(RespKind::SimpleString(value))
            }
            '$' => {
                // Bulk string
                let value = Self::consume_sized_terminated_str(data)?;
                Ok(RespKind::BulkString(value))
            }
            '=' => {
                // Verbatim string
                let value = Self::consume_sized_terminated_str(data)?;
                let (encoding, data) =
                    value.split_once(':').ok_or_else(|| RespError::InvalidStr)?;
                Ok(RespKind::VerbatimString(
                    encoding.to_string(),
                    data.to_string(),
                ))
            }
            ':' => {
                // Integer
                let value = Self::consume_terminated_int(data)?;
                Ok(RespKind::Integer(value))
            }
            ',' => {
                // Double
                let value = Self::consume_terminated_double(data)?;
                Ok(RespKind::Double(value))
            }
            '(' => {
                // Big number
                // TODO: Properly process big number instead of using a string
                let value = Self::consume_terminated_str(data)?;
                Ok(RespKind::BigNumber(value))
            }
            '-' => {
                // Simple error
                let value = Self::consume_terminated_str(data)?;
                Ok(RespKind::SimpleError(value))
            }
            '!' => {
                // Bulk error
                let value = Self::consume_sized_terminated_str(data)?;
                Ok(RespKind::BulkError(value))
            }
            '*' => {
                // Array
                let arr = Self::consume_array(data)?;
                Ok(RespKind::Array(arr))
            }
            '>' => {
                // Push
                let arr = Self::consume_array(data)?;
                Ok(RespKind::Push(arr))
            }
            '~' => {
                // Set
                let arr = Self::consume_array(data)?;
                Ok(RespKind::Set(arr))
            }
            '%' => {
                // Map
                let map = Self::consume_map(data)?;
                Ok(RespKind::Map(map))
            }
            '|' => {
                // Attributes
                let map = Self::consume_map(data)?;
                Ok(RespKind::Attributes(map))
            }
            '_' => {
                // Null
                Self::consume_terminated_str(data)?;
                Ok(RespKind::Null)
            }
            _ => Err(RespError::InvalidType),
        }
    }

    fn consume_type(data: &mut &str) -> Result<char, RespError> {
        let resp_type = data.chars().next().ok_or_else(|| RespError::InvalidType)?;
        *data = &data[1..];
        Ok(resp_type)
    }

    fn consume_array(data: &mut &str) -> Result<Vec<RespKind>, RespError> {
        let len = Self::consume_terminated_len(data)?;
        let mut arr = vec![];
        for _ in 0..len {
            let value = Self::parse_data(data)?;
            arr.push(value);
        }
        Ok(arr)
    }

    fn consume_map(data: &mut &str) -> Result<HashMap<String, RespKind>, RespError> {
        let len = Self::consume_terminated_len(data)?;
        let mut map = HashMap::new();
        for _ in 0..len {
            let key = Self::parse_data(data)?.encode();
            let value = Self::parse_data(data)?;
            map.insert(key, value);
        }
        Ok(map)
    }

    fn consume_terminated_str(data: &mut &str) -> Result<String, RespError> {
        let end_index = data
            .find("\r\n")
            .ok_or_else(|| RespError::MissingTerminator)?;
        let val = &data[..end_index];
        *data = &data[end_index + 2..];
        Ok(val.to_string())
    }

    fn consume_sized_terminated_str(data: &mut &str) -> Result<String, RespError> {
        let len_val = Self::consume_terminated_len(data)?;
        let str_val = Self::consume_terminated_str(data)?;

        if len_val == str_val.len() {
            Ok(str_val)
        } else {
            Err(RespError::MismatchedLength)
        }
    }

    fn consume_terminated_len(data: &mut &str) -> Result<usize, RespError> {
        let len_str = Self::consume_terminated_str(data)?;
        Ok(len_str.parse().map_err(|_| RespError::InvalidLength)?)
    }

    fn consume_terminated_int(data: &mut &str) -> Result<i64, RespError> {
        let int_str = Self::consume_terminated_str(data)?;
        Ok(int_str.parse().map_err(|_| RespError::InvalidInt)?)
    }

    fn consume_terminated_double(data: &mut &str) -> Result<f64, RespError> {
        let double_str = Self::consume_terminated_str(data)?;
        Ok(double_str.parse().map_err(|_| RespError::InvalidDouble)?)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn simple_string_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "+HELLO\r\n").unwrap(),
//             RespKind::SimpleString("HELLO".to_string())
//         );
//     }
//
//     #[test]
//     fn bulk_string_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "$5\r\nHELLO\r\n").unwrap(),
//             RespKind::BulkString("HELLO".to_string())
//         );
//     }
//
//     #[test]
//     fn vertatim_string_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "=9\r\ntxt:HELLO\r\n").unwrap(),
//             RespKind::VerbatimString("txt".to_string(), "HELLO".to_string())
//         );
//     }
//
//     #[test]
//     fn integer_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut ":+69\r\n").unwrap(),
//             RespKind::Integer(69)
//         );
//
//         assert_eq!(
//             RespParser::parse_data(&mut ":-420\r\n").unwrap(),
//             RespKind::Integer(-420)
//         );
//     }
//
//     #[test]
//     fn double_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut ",+1.23\r\n").unwrap(),
//             RespKind::Double(1.23)
//         );
//
//         assert_eq!(
//             RespParser::parse_data(&mut ",-4.56\r\n").unwrap(),
//             RespKind::Double(-4.56)
//         );
//     }
//
//     #[test]
//     fn big_number_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "(1234567890\r\n").unwrap(),
//             RespKind::BigNumber("1234567890".to_string())
//         );
//     }
//
//     #[test]
//     fn simple_error_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "-HELLO\r\n").unwrap(),
//             RespKind::SimpleError("HELLO".to_string())
//         );
//     }
//
//     #[test]
//     fn bulk_error_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "!5\r\nHELLO\r\n").unwrap(),
//             RespKind::BulkError("HELLO".to_string())
//         );
//     }
//
//     #[test]
//     fn array_test() {
//         // Simple array
//         assert_eq!(
//             RespParser::parse_data(&mut "*2\r\n+HELLO\r\n+WORLD\r\n").unwrap(),
//             RespKind::Array(vec![
//                 RespKind::SimpleString("HELLO".to_string()),
//                 RespKind::SimpleString("WORLD".to_string())
//             ])
//         );
//
//         // Nested array
//         assert_eq!(
//             RespParser::parse_data(&mut "*2\r\n*1\r\n+HELLO\r\n+WORLD\r\n").unwrap(),
//             RespKind::Array(vec![
//                 RespKind::Array(vec![RespKind::SimpleString("HELLO".to_string())]),
//                 RespKind::SimpleString("WORLD".to_string())
//             ])
//         );
//     }
//
//     #[test]
//     fn push_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut ">2\r\n+HELLO\r\n+WORLD\r\n").unwrap(),
//             RespKind::Push(vec![
//                 RespKind::SimpleString("HELLO".to_string()),
//                 RespKind::SimpleString("WORLD".to_string())
//             ])
//         );
//     }
//
//     #[test]
//     fn set_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "~2\r\n+HELLO\r\n+WORLD\r\n").unwrap(),
//             RespKind::Set(vec![
//                 RespKind::SimpleString("HELLO".to_string()),
//                 RespKind::SimpleString("WORLD".to_string())
//             ])
//         );
//     }
//
//     #[test]
//     fn map_test() {
//         let mut map = HashMap::new();
//         map.insert(
//             RespKind::SimpleString("HELLO".to_string()).to_string(),
//             RespKind::Integer(69),
//         );
//         map.insert(
//             RespKind::SimpleString("WORLD".to_string()).to_string(),
//             RespKind::Integer(-420),
//         );
//
//         assert_eq!(
//             RespParser::parse_data(&mut "%2\r\n+HELLO\r\n:+69\r\n+WORLD\r\n:-420\r\n").unwrap(),
//             RespKind::Map(map)
//         );
//     }
//
//     #[test]
//     fn attributes_test() {
//         let mut map = HashMap::new();
//         map.insert(
//             RespKind::SimpleString("HELLO".to_string()).to_string(),
//             RespKind::Integer(69),
//         );
//         map.insert(
//             RespKind::SimpleString("WORLD".to_string()).to_string(),
//             RespKind::Integer(-420),
//         );
//
//         assert_eq!(
//             RespParser::parse_data(&mut "|2\r\n+HELLO\r\n:+69\r\n+WORLD\r\n:-420\r\n").unwrap(),
//             RespKind::Attributes(map)
//         );
//     }
//
//     #[test]
//     fn null_test() {
//         assert_eq!(
//             RespParser::parse_data(&mut "_\r\n").unwrap(),
//             RespKind::Null
//         );
//     }
// }
