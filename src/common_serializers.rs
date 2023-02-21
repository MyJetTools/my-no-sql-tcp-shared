use std::str;

use rust_extensions::date_time::DateTimeAsMicroseconds;

pub fn serialize_byte(data: &mut Vec<u8>, v: u8) {
    data.push(v);
}

pub fn serialize_bool(data: &mut Vec<u8>, v: bool) {
    if v {
        data.push(1);
    } else {
        data.push(0);
    }
}

pub fn serialize_i32(data: &mut Vec<u8>, v: i32) {
    data.extend(&v.to_le_bytes());
}

pub fn serialize_i64(data: &mut Vec<u8>, v: i64) {
    data.extend(&v.to_le_bytes());
}

pub fn serialize_date_time_opt(data: &mut Vec<u8>, v: Option<DateTimeAsMicroseconds>) {
    if let Some(v) = v {
        serialize_i64(data, v.unix_microseconds);
    } else {
        serialize_i64(data, 0);
    }
}

pub fn serialize_pascal_string(data: &mut Vec<u8>, str: &str) {
    let str_len = str.len() as u8;
    data.push(str_len);
    data.extend(str.as_bytes());
}

pub fn serialize_list_of_arrays(data: &mut Vec<u8>, v: &Vec<Vec<u8>>) {
    let array_len = v.len() as i32;
    serialize_i32(data, array_len);

    for arr in v {
        serialize_byte_array(data, arr);
    }
}

pub fn serialize_list_of_pascal_strings(data: &mut Vec<u8>, v: &Vec<String>) {
    let array_len = v.len() as i32;
    serialize_i32(data, array_len);

    for str in v {
        serialize_pascal_string(data, str);
    }
}

pub fn serialize_byte_array(data: &mut Vec<u8>, v: &[u8]) {
    let array_len = v.len() as i32;
    serialize_i32(data, array_len);
    data.extend(v);
}
