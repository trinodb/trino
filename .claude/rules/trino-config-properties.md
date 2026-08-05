---
paths:
  - "**/*Config.java"
  - "**/*SessionProperties.java"
---

# Trino — configuration and session properties

Naming:
- Config property names use **dashes**, e.g. `hive.max-partitions-per-scan`.
- Session property names use **snake_case**, e.g. `max_partitions_per_scan`.

Adding a property:
- Every `@Config` setter gets an `@ConfigDescription("…")`.
- Every session property registration includes a description.
- Credentials and other secrets get `@ConfigSecuritySensitive` so values are redacted in logs
  and info endpoints.

Renaming a config:
- Add a `@Deprecated` setter with `@LegacyConfig(value = "old.name", replacedBy = "new.name")`
  that writes to the same field as the new `@Config("new.name")` setter. The old name keeps
  working as a backward-compatible alias.

Removing a config:
- Add the current name **and** any `@LegacyConfig` names to `@DefunctConfig` on the class so
  startup fails loudly if the config is still set.
- Remove the matching session property from `SystemSessionProperties.java` (or the connector's
  session-properties class) if one exists.

Testing a new config:
- Add a matching `TestMyConfig` using Airlift's `ConfigAssertions` — see existing `Test*Config`
  classes for the `testDefaults()` / `testExplicitPropertyMappings()` pattern.

Other conventions:
- Validation annotations (`@NotNull`, `@Min`, `@MinDuration`, etc.) go on setters, not fields.
- Don't store the config object as a field — read values in the constructor and keep those
  instead.
