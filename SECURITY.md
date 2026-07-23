# Security policy

## Supported versions

Security fixes are released for the latest `1.x` version. Operators should stay
on the newest published release.

## Reporting a vulnerability

Please use the repository's **Security** tab to submit a private vulnerability
report. Do not include secrets, wallet material, private prompts, completions, or
production host details in a public issue.

Include the affected version, impact, a minimal reproduction, and any suggested
mitigation. Maintainers will acknowledge a valid report privately and coordinate
disclosure with the reporter.

## Deployment boundary

Reliquary Fleet is an operator dashboard, not an authentication gateway. It binds
to `127.0.0.1` by default and refuses a non-loopback bind unless
`--allow-remote` is supplied. For remote access, place it behind an authenticated
TLS reverse proxy or use an SSH tunnel. Never expose the dashboard directly to
the public internet: its read APIs contain operational topology and performance
data.

Prefer a credential-free, cache-enabled HTTPS archive origin for distributed
installs. If a private R2 bucket is required, keep credentials out of YAML
where possible and scope them to `GetObject` on the archive prefix. Exhaustive
bucket LIST is disabled by default.

Validator SSH is optional. When enabled, use a dedicated unprivileged identity
that can read only the intended container logs and metadata; never distribute
a shared production SSH key as part of public onboarding. Miner and lab keys
should likewise be per-operator and narrowly scoped. The generated config is
mode `0600`; `reliquary-fleet doctor` warns about unsafe local permissions.

Fleet has no telemetry or hosted control-plane dependency. Browser requests are
same-origin and local by default. Shared validator reads use bounded independent
cadences, incremental cursors, jitter, `Retry-After`, and exponential backoff.
