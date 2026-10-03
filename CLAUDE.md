# Trino — Claude guidance

For other topics not covered here (Web UI build, release process, IDE setup rationale),
see [`.github/DEVELOPMENT.md`](.github/DEVELOPMENT.md).

## Building

Fastest full build and install:

```bash
./mvnw clean install -T 2C -nsu -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true
```

It skips tests, Javadoc, and the airbase checks — run `./mvnw validate` before opening a PR.

## Java formatting

Run `./mvnw airstyle:format` after Java edits — the `airstyle-maven-plugin` (`io.airlift:airstyle-maven-plugin`)
applies the canonical Airstyle scheme, which is what CI checks. Scope a single file with
`./mvnw -pl <module> airstyle:format -Dincludes=**/FileName.java`, and use `airstyle:check` to verify
without rewriting.

## Commits and pull requests

Pull request descriptions use [`.github/pull_request_template.md`](.github/pull_request_template.md).

CI enforces these rules, so check them before pushing:

- Commit messages: the [check-commit-messages policy](https://github.com/airlift/github-actions/tree/main/check-commit-messages#policy),
  run by the `check-commit-messages` job in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- Commit structure: the `check-commits-dispatcher` and `check-commit` jobs in the same file.
- Pull request description: [`.github/workflows/pr-description-check.yml`](.github/workflows/pr-description-check.yml).
