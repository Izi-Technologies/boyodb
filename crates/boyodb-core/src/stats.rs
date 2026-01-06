use crate::replication::{ColumnStats, PrimitiveValue};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use arrow::compute::kernels::aggregate;

pub fn calculate_column_stats(array: &ArrayRef) -> ColumnStats {
    let null_count = array.null_count() as u64;
    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(PrimitiveValue::Int64),
                max: aggregate::max(arr).map(PrimitiveValue::Int64),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Int64(v as i64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(|v| PrimitiveValue::Float64(v as f64)),
                max: aggregate::max(arr).map(|v| PrimitiveValue::Float64(v as f64)),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            ColumnStats {
                min: aggregate::min(arr).map(PrimitiveValue::Float64),
                max: aggregate::max(arr).map(PrimitiveValue::Float64),
                bloom_filter: None,
                null_count,
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            
            // Compute Bloom filter for strings
            let mut bloom = bloomfilter::Bloom::new_for_fp_rate(arr.len().max(1), 0.01);
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    bloom.set(&arr.value(i));
                }
            }
            // Use bloom_utils from engine? No, stats.rs is in core, bloom_utils is in core.
            let bloom_bytes = crate::bloom_utils::serialize_bloom(&bloom).ok();

            ColumnStats {
                min: aggregate::min_string(arr).map(|v| PrimitiveValue::String(v.to_string())),
                max: aggregate::max_string(arr).map(|v| PrimitiveValue::String(v.to_string())),
                bloom_filter: bloom_bytes,
                null_count,
            }
        }
        DataType::Boolean => {
             let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
             ColumnStats {
                 min: aggregate::min_boolean(arr).map(PrimitiveValue::Boolean),
                 max: aggregate::max_boolean(arr).map(PrimitiveValue::Boolean),
                 bloom_filter: None,
                 null_count,
             }
        }
        DataType::Dictionary(_, _) => {
            let values = arrow::array::make_array(array.to_data().child_data()[0].clone());
            let mut stats = calculate_column_stats(&values);
            stats.null_count = null_count;
            stats
        }
        _ => ColumnStats {
            min: None,
            max: None,
            bloom_filter: None,
            null_count,
        },
    }
}
