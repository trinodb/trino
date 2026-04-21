# Trino — Claude guidance

**Before writing Java code, you must first read [`.github/DEVELOPMENT.md`](.github/DEVELOPMENT.md)
in full** — it's the authoritative source for code-style rules (mocks, `var`, switch statements,
method naming, `format()`, `TrinoException` error codes, AssertJ, Guava immutables, and more).
This file intentionally does not duplicate those rules; skipping the read means missing them.
Violations are also caught mechanically by modernizer
([`.mvn/modernizer/violations.xml`](.mvn/modernizer/violations.xml)), checkstyle (from Airbase),
and IntelliJ inspections via `mcp__idea__get_file_problems`.

For other topics not covered here (Web UI build, release process, Vector API, IDE setup rationale),
see the same `DEVELOPMENT.md`.

## JetBrains MCP server

Trino is developed in IntelliJ. If you run Claude Code with the JetBrains MCP server enabled,
the assistant can drive the IDE directly. Install: https://github.com/JetBrains/mcp-jetbrains.

When available, Claude should prefer these over shell equivalents:
- `mcp__idea__reformat_file` after Java edits — applies the Airlift formatter; Claude cannot
  reproduce it by hand.
- `mcp__idea__get_file_problems` before committing — surfaces IntelliJ inspection results
  (error-prone, unused imports, nullability) without a full Maven build.
- `mcp__idea__search_symbol` / `mcp__idea__get_symbol_info` for symbol navigation in a codebase
  with many overloaded names like `Metadata`, `Session`, `Block`.
- `mcp__idea__rename_refactoring` for API renames — safer than text substitution.

## Java formatting

Run `mcp__idea__reformat_file` after every Java edit — it applies the Airlift scheme imported into
IntelliJ. Rules not covered by the formatter:

- No wildcard imports (e.g. `import io.trino.spi.*`) — checkstyle catches these on build; easier
  to avoid writing them.
- Braces required around single-statement `if` / `for` / `while` bodies — the formatter does not
  add missing braces.
- No `@author` in JavaDoc — commit history is the record.

Topic-specific conventions live under [`.claude/rules/`](.claude/rules/) and auto-load when Claude
reads matching files (e.g. `*Config.java` triggers the config-properties rule).
