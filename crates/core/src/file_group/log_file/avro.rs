/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use apache_avro::AvroResult;
use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema as AvroSchema, from_avro_datum};
use std::io::Read;

pub struct AvroDataBlockContentReader<R: Read> {
    reader: R,
    writer_schema: AvroSchema,
    remaining_records: u32,
}

impl<R: Read> AvroDataBlockContentReader<R> {
    pub fn new(reader: R, writer_schema: &AvroSchema, num_records: u32) -> Self {
        Self {
            reader,
            writer_schema: writer_schema.clone(),
            remaining_records: num_records,
        }
    }
}

impl<R: Read> Iterator for AvroDataBlockContentReader<R> {
    type Item = AvroResult<AvroValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_records == 0 {
            return None;
        }

        self.remaining_records -= 1;

        let mut record_content_length = [0u8; 4];
        match self.reader.read_exact(&mut record_content_length) {
            Ok(_) => {}
            Err(e) => {
                return Some(Err(apache_avro::Error::new(
                    apache_avro::error::Details::ReadBytes(e),
                )));
            }
        }

        let record_content_length = u32::from_be_bytes(record_content_length);

        let mut record_reader = (&mut self.reader).take(record_content_length as u64);

        let result = from_avro_datum(&self.writer_schema, &mut record_reader, None);

        // Drain any bytes not consumed by from_avro_datum so the underlying reader's
        // position is always at the start of the next record's 4-byte length prefix.
        std::io::copy(&mut record_reader, &mut std::io::sink()).ok();

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_error_on_truncated_data() {
        // Create a simple Avro schema
        let schema = AvroSchema::parse_str(r#"{"type": "null"}"#).unwrap();

        // Create a reader with only 2 bytes when we need 4 bytes for record length
        let truncated_data = vec![0u8, 1u8];
        let reader = Cursor::new(truncated_data);

        // Create reader expecting 1 record but with insufficient data
        let mut avro_reader = AvroDataBlockContentReader::new(reader, &schema, 1);

        // Should return an error because read_exact fails
        let result = avro_reader.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_drain_leftover_bytes_between_records() {
        use apache_avro::to_avro_datum;
        use apache_avro::types::Value;

        let schema = AvroSchema::parse_str(r#"{"type":"string"}"#).unwrap();

        let hello = to_avro_datum(&schema, Value::String("hello".into())).unwrap();
        let world = to_avro_datum(&schema, Value::String("world".into())).unwrap();

        let mut data: Vec<u8> = Vec::new();
        // First record: content_length is 3 bytes larger than actual datum —
        // simulates a writer that pads/over-reports the length.
        let padded_len = hello.len() as u32 + 3;
        data.extend_from_slice(&padded_len.to_be_bytes());
        data.extend_from_slice(&hello);
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // padding bytes
        // Second record: exact length
        data.extend_from_slice(&(world.len() as u32).to_be_bytes());
        data.extend_from_slice(&world);

        let mut reader = AvroDataBlockContentReader::new(Cursor::new(data), &schema, 2);
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::String("hello".into())
        );
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::String("world".into())
        );
        assert!(reader.next().is_none());
    }
}
