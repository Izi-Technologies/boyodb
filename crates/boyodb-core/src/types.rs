use std::ffi::c_void;
use tracing::warn;

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BoyodbStatus {
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    Internal = 3,
    NotImplemented = 4,
    Io = 5,
    Timeout = 6,
}

impl BoyodbStatus {
    pub fn from_result(result: &Result<(), crate::engine::EngineError>) -> Self {
        match result {
            Ok(_) => BoyodbStatus::Ok,
            Err(crate::engine::EngineError::InvalidArgument(_)) => BoyodbStatus::InvalidArgument,
            Err(crate::engine::EngineError::NotFound(_)) => BoyodbStatus::NotFound,
            Err(crate::engine::EngineError::NotImplemented(_)) => BoyodbStatus::NotImplemented,
            Err(crate::engine::EngineError::Internal(_)) => BoyodbStatus::Internal,
            Err(crate::engine::EngineError::Io(_)) => BoyodbStatus::Io,
            Err(crate::engine::EngineError::Timeout(_)) => BoyodbStatus::Timeout,
            Err(crate::engine::EngineError::Remote(_)) => BoyodbStatus::Internal,
            Err(crate::engine::EngineError::Configuration(_)) => BoyodbStatus::Internal,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct OwnedBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
    pub destructor: Option<extern "C" fn(*mut c_void, *mut u8, usize, usize)>,
    pub destructor_state: *mut c_void,
}

impl OwnedBuffer {
    pub fn from_vec(mut data: Vec<u8>) -> Self {
        let owned_len = data.len();
        let owned_cap = data.capacity();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);
        OwnedBuffer {
            data: ptr,
            len: owned_len,
            capacity: owned_cap,
            destructor: Some(Self::drop_vec),
            destructor_state: std::ptr::null_mut(),
        }
    }

    extern "C" fn drop_vec(_state: *mut c_void, data: *mut u8, len: usize, cap: usize) {
        if data.is_null() {
            warn!("OwnedBuffer.drop_vec called with null data; skipping free");
            return;
        }
        if len > cap {
            warn!(len, cap, "OwnedBuffer.drop_vec received len > cap; skipping free to avoid UB");
            return;
        }
        unsafe {
            let _ = Vec::from_raw_parts(data, len, cap);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn drop_vec_noops_on_null() {
        OwnedBuffer::drop_vec(ptr::null_mut(), ptr::null_mut(), 0, 0);
    }

    #[test]
    fn drop_vec_rejects_len_gt_cap() {
        let mut v = Vec::with_capacity(1);
        let cap = v.capacity();
        let ptr = v.as_mut_ptr();
        std::mem::forget(v);

        OwnedBuffer::drop_vec(ptr::null_mut(), ptr, cap + 1, cap);

        unsafe {
            // Reclaim to avoid leak since drop_vec early-returned
            let _ = Vec::from_raw_parts(ptr, 0, cap);
        }
    }
}
