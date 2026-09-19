# Security

> Synthetic data throughout. No real customer, meter or tariff exists in this
> repository, and nothing here has been deployed to a production environment.

This is a portfolio project, so the honest framing is: these are the controls
that *are* implemented and verifiable by reading the code, followed by the ones
that would be required before this ran anywhere real.

---

## 1. Secrets

**No credential is in the repository.** The controls, in order of how much they
are worth:

| Control | Where |
| --- | --- |
| `.env` is git-ignored; `.env.example` holds placeholders only | `.gitignore`, `.env.example` |
| The ignore rule is `.env` **and** `.env.*` with a `!.env.example` exception | `.gitignore` |
| The password is a Pydantic `SecretStr` | `config.py` |
| Logs use `safe_dsn`, never `dsn` | `config.py`, `db/engine.py` |
| Compose defaults are obviously-local placeholders (`change_me_local_only`) | `docker-compose.yml` |

`SecretStr` is the one that does real work. A plain `str` password leaks through
any `repr()` of the settings object -- which is exactly what gets printed by an
unhandled exception, a debugger, or a well-meant `logger.info("config=%s",
settings)`. `SecretStr.__repr__` returns `**********`, so the leak has to be
deliberate:

```python
class Settings(BaseSettings):
    db_password: SecretStr = SecretStr("change_me_local_only")

    @property
    def dsn(self) -> str:
        """SQLAlchemy URL including the password -- never log this."""

    @property
    def safe_dsn(self) -> str:
        """Connection string with the password masked -- safe to log."""
```

Two properties rather than one, so that the safe form is the convenient one.

The CI workflow contains `POSTGRES_PASSWORD: ci_test_password` in plain text.
That is deliberate and commented in place: it belongs to a service container
that is created and destroyed with the job, is reachable only from inside the
runner, and has no counterpart anywhere else. Putting it in a GitHub secret
would imply it protects something.

**What is not done.** There is no secret manager integration (Key Vault,
Secrets Manager, Vault), no rotation, and no per-environment separation. For a
single-container demo, environment variables are the correct level; for anything
multi-tenant they are not, and the gap is listed in §6.

---

## 2. SQL injection

Every **value** that reaches the database is a bound parameter. There is no
f-string interpolation of user input into SQL anywhere in `src/`.

Identifiers are the harder half, because they cannot be bound. Three cases
exist, and each is closed differently:

**Schema names** come from configuration and are validated on the way in:

```python
@field_validator("raw_schema", "core_schema", "mart_schema", "meta_schema")
# must match [a-z_][a-z0-9_]*
```

An attempt to set `HELIOS_CORE_SCHEMA='core; DROP SCHEMA raw CASCADE'` fails at
process start with a validation error, before a connection is opened. The SQL
files use `${RAW}` / `${CORE}` / `${MART}` / `${META}` placeholders substituted
by `db/sql_files.py` only after that validation.

**Partition names** are generated, never accepted: `meter_reading_%Y%m`, derived
from a parsed `datetime`. A string that is not a timestamp never becomes a
partition name.

**Source names** in the CLI are checked against the registry
(`DEFAULT_SOURCE_ORDER`) -- an allow-list -- and then used as bound parameters,
not identifiers.

The one place where a name comes from outside is `list_reading_partitions()`,
which reads from `pg_catalog`. Identifiers that the database itself reports are
as trustworthy as identifiers get.

---

## 3. The untrusted-input boundary

The pipeline treats the upstream API as hostile, because in production it is
merely *unreliable*, which has the same consequences.

| Threat | Control |
| --- | --- |
| Malformed or hostile payload | Contract validation before anything is buffered; violations are dead-lettered with the failing field |
| Payload too large / unbounded page | `api_page_size` capped at 50 000, enforced server-side; an oversized request is a **permanent** error, not a retried one |
| Type confusion (`"1234.50"` vs `1234.5`) | Contract normalisation to a canonical type before hashing or storing |
| A record that never becomes valid | `HELIOS_DLQ_MAX_ATTEMPTS`, then `ABANDONED` -- a poison record cannot loop forever |
| Malformed cursor | `decode_cursor` raises `ValueError`, the router returns **400**, not 500 -- a bad cursor is the client's mistake and should not look like a server fault |
| Unbounded retry against a failing source | Retry budget plus `Retry-After`; `RetryBudgetExhausted` fails the run with the watermark unmoved |

Validation happens **before** the buffer, so `raw` only ever holds payloads that
satisfied a known, versioned contract. That is what lets promotion be a plain
SQL cast with no defensive `CASE` statements -- the defence already happened.

---

## 4. The container

```dockerfile
RUN useradd --create-home --shell /bin/bash --uid 10001 helios
USER helios
```

- **Multi-stage build.** Compilers, pip caches and build metadata stay in the
  builder stage; the runtime image gets the virtual environment and the
  application only.
- **Non-root, UID 10001.** A data pipeline has no reason to hold root inside its
  own container. CI asserts it rather than trusting the Dockerfile:
  ```bash
  test "$(docker run --rm --entrypoint id …:ci -u)" != "0"
  ```
- **Slim base**, `python:3.12-slim-bookworm`, with only `postgresql-client` and
  `curl` added and apt lists removed in the same layer.
- **`.dockerignore`** excludes `.env`, `.git`, `data/`, caches and test
  artefacts, so a local `.env` cannot be copied into an image by accident.
- **Healthcheck** is `helios doctor`, which already exits non-zero when the
  database is unreachable.
- **No secret in any layer.** Nothing is `COPY`d that is not in git, and
  `ENV` carries configuration, never credentials.

---

## 5. Database privileges

The application connects as `helios_app`, which owns its four schemas. It does
**not** connect as `postgres`.

`helios reset` drops and recreates the schemas, which is why it is a separate,
explicitly-named command with a confirmation prompt rather than a flag on
`init-db`. Destructive operations should be hard to reach by accident.

**What is not done.** There is no read-only role for the BI consumer, and no
row-level security. Both are listed in §6; for a single-user demo warehouse they
would be ceremony, and pretending otherwise would be the dishonest option.

---

## 6. What this does not do

Stated plainly, because a security section that lists only what is present is a
marketing document.

| Gap | What production would require |
| --- | --- |
| Secrets in environment variables | A managed secret store with rotation and per-environment scoping |
| No authentication on the pipeline API | mTLS or OAuth2 between the pipeline and the upstream; an authenticated `/pipeline/*` surface |
| Single application role | Separate `helios_ingest` (write) and `helios_read` (select on `mart` only) roles |
| No encryption at rest configured | Disk or tablespace encryption, and TLS enforced on the database connection (`sslmode=verify-full`) |
| No audit trail of *who* ran what | `meta.pipeline_run` records what ran and when, not which principal triggered it |
| No dependency scanning in CI | `pip-audit` or Dependabot, plus image scanning with Trivy |
| PII | None here -- meter telemetry is synthetic and has no subscriber attached. Real metering data is personal data under GDPR, and would need retention limits, pseudonymisation of the site/customer link, and a lawful basis recorded |
| Secret scanning | `gitleaks` in a pre-commit hook; the `.gitignore` is a convention, and a hook is a control |

None of these are hard to add. They are absent because adding them without an
environment to enforce them against would produce configuration that has never
been exercised -- which reads as a control and behaves as decoration.

---

## 7. If you find something

This is a portfolio repository with no deployment and no users. Open an issue.
