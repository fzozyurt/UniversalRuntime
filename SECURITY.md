# Security Policy

Phase 1 security baseline is documented in `AGENTS.md`. Report vulnerabilities privately to repository maintainers.

Never commit:

- provider API keys,
- database passwords,
- private certificates,
- production prompts containing secrets,
- raw user JWTs or session exports.

Security-sensitive changes require tests for redaction, authorization boundary assumptions, network exposure and dependency behavior.
