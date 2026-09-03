//! Strict URL construction for the SDK's HTTP clients.
//!
//! Every HTTP path is assembled with [`reqwest::Url`]'s path and query APIs
//! rather than string formatting, so caller-supplied values (topic names,
//! subjects, group IDs, branch IDs) are percent-encoded instead of being able
//! to inject extra path segments, query parameters, or fragments.
//!
//! Path segments that are exactly `.` or `..` are rejected outright: they are
//! never valid Streamline identifiers, and a server that decodes them before
//! routing could otherwise be walked outside the intended resource.

use crate::error::{Error, ErrorKind, Result};
use reqwest::Url;

/// Rejects path segments that cannot safely address a resource.
///
/// An empty segment would collapse (`//`) or produce a trailing slash, and the
/// dot segments `.` and `..` are relative-path operators rather than
/// identifiers.
pub(crate) fn validate_path_segment(kind: &str, segment: &str) -> Result<()> {
    if segment.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!("{kind} cannot be empty"),
        )
        .with_hint("Provide a non-empty value"));
    }

    if segment == "." || segment == ".." {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!("{kind} cannot be '{segment}'"),
        )
        .with_hint("'.' and '..' are relative path segments and are never valid identifiers"));
    }

    Ok(())
}

/// Builds an absolute URL from a base, path segments, and query parameters.
///
/// Segments are appended to the base URL's existing path and percent-encoded
/// by [`reqwest::Url`]; query parameters are appended with
/// `query_pairs_mut`, so reserved characters (`/`, `?`, `#`, `&`, `=`, `%`,
/// spaces) in caller values cannot change the request target.
///
/// # Errors
/// Returns [`ErrorKind::InvalidConfiguration`] when the base URL is not an
/// absolute `http`/`https` URL, or when any segment is empty or a dot segment.
pub(crate) fn build_url(base: &str, segments: &[&str], query: &[(&str, String)]) -> Result<Url> {
    let mut url = Url::parse(base).map_err(|error| {
        Error::new(
            ErrorKind::InvalidConfiguration,
            format!("Invalid base URL '{base}': {error}"),
        )
        .with_hint("Use an absolute URL such as http://localhost:9094")
    })?;

    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!(
                "Base URL '{base}' must use the http or https scheme, got '{}'",
                url.scheme()
            ),
        ));
    }

    for segment in segments {
        validate_path_segment("URL path segment", segment)?;
    }

    {
        let mut path = url.path_segments_mut().map_err(|_| {
            Error::new(
                ErrorKind::InvalidConfiguration,
                format!("Base URL '{base}' cannot be a base for request paths"),
            )
        })?;
        // Drop the empty segment produced by a trailing slash so appended
        // segments do not create a doubled separator.
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
    }

    if query.is_empty() {
        url.set_query(None);
    } else {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    url.set_fragment(None);

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_simple_path() {
        let url = build_url("http://localhost:9094", &["v1", "cluster"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/v1/cluster");
    }

    #[test]
    fn test_preserves_base_path_prefix() {
        let url = build_url("http://localhost:9094/api", &["v1", "subjects"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/api/v1/subjects");
    }

    #[test]
    fn test_trailing_slash_does_not_double_separator() {
        let url = build_url("http://localhost:9094/api/", &["v1"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/api/v1");
    }

    #[test]
    fn test_rejects_dot_segments() {
        for segment in [".", ".."] {
            let error = build_url("http://localhost:9094", &["v1", segment], &[]).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
            assert!(error.message.contains(segment), "{}", error.message);
        }
    }

    #[test]
    fn test_rejects_empty_segment() {
        let error = build_url("http://localhost:9094", &["v1", ""], &[]).unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[test]
    fn test_encoded_dot_segments_are_not_path_operators() {
        // A literal "%2e%2e" is not the dot segment "..", and must stay
        // encoded (the '%' itself is escaped) rather than climbing the path.
        let url = build_url("http://localhost:9094", &["v1", "%2e%2e"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/v1/%252e%252e");
        assert_eq!(
            url.path_segments().unwrap().collect::<Vec<_>>(),
            vec!["v1", "%252e%252e"]
        );
    }

    #[test]
    fn test_reserved_characters_in_segments_are_encoded() {
        let url = build_url(
            "http://localhost:9094",
            &["v1", "topics", "a/../b?x=1#frag"],
            &[],
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:9094/v1/topics/a%2F..%2Fb%3Fx=1%23frag"
        );
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
        assert_eq!(url.path_segments().unwrap().count(), 3);
    }

    #[test]
    fn test_space_and_unicode_segments_are_encoded() {
        let url = build_url("http://localhost:9094", &["v1", "hello wörld"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/v1/hello%20w%C3%B6rld");
    }

    #[test]
    fn test_query_parameters_are_encoded() {
        let url = build_url(
            "http://localhost:9094",
            &["v1", "inspect"],
            &[
                ("partition", "0".to_string()),
                ("filter", "a&b=c#d /e".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(url.query(), Some("partition=0&filter=a%26b%3Dc%23d+%2Fe"));
        assert_eq!(url.path(), "/v1/inspect");
    }

    #[test]
    fn test_rejects_non_http_scheme() {
        let error = build_url("file:///etc/passwd", &["v1"], &[]).unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[test]
    fn test_rejects_relative_base() {
        let error = build_url("localhost:9094", &["v1"], &[]).unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfiguration);
    }

    #[test]
    fn test_base_dot_segments_are_normalized_away() {
        let url = build_url("http://localhost:9094/api/../admin", &["v1"], &[]).unwrap();
        assert_eq!(url.as_str(), "http://localhost:9094/admin/v1");
    }

    #[test]
    fn test_validate_path_segment_messages() {
        let error = validate_path_segment("subject", "").unwrap_err();
        assert!(error.message.contains("subject"));
        let error = validate_path_segment("subject", "..").unwrap_err();
        assert!(error.message.contains(".."));
        assert!(validate_path_segment("subject", "orders-value").is_ok());
        assert!(validate_path_segment("subject", ".hidden").is_ok());
    }
}
