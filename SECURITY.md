# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.4.x   | :white_check_mark: |
| < 0.4   | :x:                |

## Reporting a Vulnerability

Please report security vulnerabilities to **security@streamlinelabs.dev**.

**Do NOT open public issues for security vulnerabilities.**

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 5 business days
- **Fix Timeline**: Communicated after assessment

We follow responsible disclosure practices and will credit reporters (with permission) in our release notes.

## Security Best Practices

For production deployments, use a supported SDK transport configuration and
review the security guidance in the main
[Streamline repository](https://github.com/streamlinelabs/streamline/security).

The Rust SDK 0.4.x rejects TLS and SASL broker configurations because those
transports are not implemented. Do not place this version on an untrusted
network under the assumption that the `tls` or `sasl` compatibility features
enable encryption or authentication.
