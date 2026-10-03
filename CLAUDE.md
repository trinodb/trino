# Trino — Claude guidance

**Before writing Java code, you must first read [`.github/DEVELOPMENT.md`](.github/DEVELOPMENT.md)
in full** — it's the authoritative source for code-style rules (mocks, `var`, switch statements,
method naming, `format()`, `TrinoException` error codes, AssertJ, Guava immutables, and more).
This file intentionally does not duplicate those rules; skipping the read means missing them.
Violations are also caught mechanically by modernizer
([`.mvn/modernizer/violations.xml`](.mvn/modernizer/violations.xml)) and checkstyle (from Airbase).

For other topics not covered here (Web UI build, release process, Vector API, IDE setup rationale),
see the same `DEVELOPMENT.md`.

## Building

Fastest full build and install:

```bash
./mvnw clean install -T 2C -nsu -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true
```

It skips tests, Javadoc, and the airbase checks — run `./mvnw validate` before opening a PR.

## Java formatting

Run `mvnd airstyle:format` after Java edits — the `airstyle-maven-plugin` (`io.airlift:airstyle-maven-plugin`)
applies the canonical Airstyle scheme, which is what CI checks. Scope a single file with
`mvnd -pl <module> airstyle:format -Dincludes=**/FileName.java`, and use `airstyle:check` to verify
without rewriting. Rules not covered by the formatter:

- No wildcard imports (e.g. `import io.trino.spi.*`) — checkstyle catches these on build; easier
  to avoid writing them.
- Braces required around single-statement `if` / `for` / `while` bodies — the formatter does not
  add missing braces.
- No `@author` in JavaDoc — commit history is the record.

Topic-specific conventions live under [`.claude/rules/`](.claude/rules/) and auto-load when Claude
reads matching files (e.g. `*Config.java` triggers the config-properties rule).

## Commits and pull requests

Pull request descriptions use [`.github/pull_request_template.md`](.github/pull_request_template.md).

CI enforces these rules, so check them before pushing:

- Commit messages: the [check-commit-messages policy](https://github.com/airlift/github-actions/tree/main/check-commit-messages#policy),
  run by the `check-commit-messages` job in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- Commit structure: the `check-commits-dispatcher` and `check-commit` jobs in the same file.
- Pull request description: [`.github/workflows/pr-description-check.yml`](.github/workflows/pr-description-check.yml).
