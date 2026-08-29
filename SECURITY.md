# Security Policy

## Supported versions

Security fixes are provided for the latest v1 release line.

| Version | Supported |
|---|---|
| Latest `1.x` release | Yes |
| `0.x` releases | No |

Before reporting a vulnerability, confirm that it is reproducible on the
latest release of the affected Goncordia module.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's
[private vulnerability reporting form](https://github.com/kirimatt/goncordia/security/advisories/new)
to send the report confidentially. Include:

- The affected module, backend, and version.
- The security impact and realistic attack scenario.
- Reproduction steps or a minimal proof of concept.
- Any known mitigations or workarounds.
- Whether the vulnerability has been disclosed elsewhere.

Do not include real credentials, personal data, or production records. The
maintainers will acknowledge the report, investigate it, and coordinate a fix
and disclosure with the reporter. Please allow time for a patched release to be
prepared before public disclosure.

## Scope

Reports are especially useful for authorization bypasses in the admin API,
cross-tenant data exposure, unsafe payload handling, lease/fencing failures
that violate documented execution guarantees, dependency vulnerabilities with
a reachable call path, and release or supply-chain compromise.

Availability limitations and documented backend consistency trade-offs are not
security vulnerabilities unless they enable an attacker to violate a stated
security boundary.
