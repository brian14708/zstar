//! Native location parsing and transport configuration shared by both modes.
use pyo3::prelude::*;
use std::path::PathBuf;

pub enum Location {
    File(PathBuf),
    Http(String),
    S3 { bucket: String, key: String },
}
pub fn location(value: String) -> PyResult<Location> {
    if !value.contains("://")
        && !value
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("file:"))
    {
        return Ok(Location::File(value.into()));
    }
    let url = url::Url::parse(&value)
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("invalid location URL"))?;
    match url.scheme() {
        "file" if url.query().is_none() && url.fragment().is_none() => {
            Ok(Location::File(url.to_file_path().map_err(|()| {
                pyo3::exceptions::PyValueError::new_err("invalid local file URL")
            })?))
        }
        "http" | "https" => Ok(Location::Http(value)),
        "s3" if url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none() =>
        {
            let bucket = url
                .host_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("S3 bucket is required"))?
                .to_owned();
            let key = zstar::s3::Path::from_url_path(url.path())
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("invalid S3 object key"))?
                .to_string();
            if key.is_empty() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "S3 object key is required",
                ));
            }
            Ok(Location::S3 { bucket, key })
        }
        scheme => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported location scheme: {scheme}"
        ))),
    }
}
