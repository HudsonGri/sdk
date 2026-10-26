pub struct Always;

#[cfg(not(feature = "extra"))]
pub fn overloaded(value: u64) -> u64 { value }

#[cfg(feature = "extra")]
pub fn overloaded(value: &str) -> usize { value.len() }

#[cfg(feature = "extra")]
pub struct FeatureOnly;
