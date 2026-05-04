pub mod protocol;
pub mod shared_mem;

pub use protocol::{
    deserialize_features, deserialize_signal, serialize_features, serialize_signal,
    FeatureSnapshot, PythonSignal, SignalDirection, FEATURES_BYTES, FEATURE_COUNT,
    SIGNAL_BYTES,
};
pub use shared_mem::{
    create_features_mmap, default_features_path, default_signal_path,
    features_writer_loop, open_signal_mmap, signal_reader_loop,
};
