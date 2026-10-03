# Development

In this document you can find information about developing Trino.

* [Trino organization](#trino-organization)
* [Trino developer guide](#trino-developer-guide)
* [Code style](#code-style)
* [Building](#building)
* [Additional IDE configuration](#additional-ide-configuration)
* [Building docs](#building-docs)
* [Building the Web UI](#building-the-web-ui)
* [Releases](#releases)

## Trino organization

Learn about development for all Trino organization projects:

* [Vision](https://trino.io/development/vision)
* [Contribution process](https://trino.io/development/process#contribution-process)
* [Pull request and commit guidelines](https://trino.io/development/process#pull-request-and-commit-guidelines)
* [Release note guidelines](https://trino.io/development/process#release-note-guidelines)

Further information in the [development section of the
website](https://trino.io/development) includes different roles, like
contributors, reviewers, and maintainers, related processes, and other aspects.

## Trino developer guide

See [the Trino developer guide](https://trino.io/docs/current/develop.html) for
information about the SPI, implementing connectors and other plugins,
the client protocol, writing tests and other lower level details.

## Code Style

We recommend you use IntelliJ as your IDE. Code style is managed through [airstyle](https://github.com/airlift/airstyle).

To run airstyle and other maven checks before opening a PR: `./mvnw validate`

In addition to those you should also adhere to the [code style rules](CODE_STYLE.md)
and the [configuration property rules](CONFIG_PROPERTIES.md).

## Keep pom.xml clean and sorted

There are several plugins in place to keep pom.xml clean.
Your build may fail if:
 - dependencies or XML elements are not ordered correctly
 - overall pom.xml structure is not correct

Many such errors may be fixed automatically by running the following:
`./mvnw sortpom:sort`

## Building

The fastest way to build and install the whole project:

```bash
./mvnw clean install -T 2C -nsu -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true
```

This builds with two threads per core, skips snapshot update checks, tests, Javadoc, and the
airbase checks (checkstyle, modernizer, dependency analysis). Run `./mvnw validate` separately
before opening a PR to get those checks back.

## Additional IDE configuration

When using IntelliJ to develop Trino, we recommend starting with all of the
default inspections, with some modifications.

Enable the following inspections:

- ``Java | Class structure | Utility class is not 'final'``,
- ``Java | Class structure | Utility class without 'private' constructor``,
- ``Java | Control flow issues | Redundant 'else'`` (including
  ``Report when there are no more statements after the 'if' statement`` option).

Disable the following inspections:

- ``Java | Abstraction issues | 'Optional' used as field or parameter type``,
- ``Java | Code style issues | Local variable or parameter can be 'final'``,
- ``Java | Data flow | Boolean method is always inverted``,
- ``Java | Performance | Call to 'Arrays.asList()' with too few arguments``.

Update the following inspections:

- Remove ``com.google.common.annotations.Beta`` from ``JVM languages | Unstable API usage``.

Enable errorprone ([Error Prone Installation#IDEA](https://errorprone.info/docs/installation#intellij-idea)):
- Install ``Error Prone Compiler`` plugin from marketplace,
- Check the `errorprone-compiler` profile in the Maven tab

This should be enough - IDEA should automatically copy the compiler options from
the POMs to each module. If that doesn't work, you can do it manually:

- In ``Java Compiler`` tab, select ``Javac with error-prone`` as the compiler,
- Update ``Additional command line parameters`` and copy the contents of
  ``compilerArgs`` in the top-level POM (except for ``-Xplugin:ErrorProne``)
  there
  - Remove the XML comments...
  - ...except the ones which denote checks which fail in IDEA, which you should
    "unwrap"
- Remove everything from the list under ``Override compiler parameters per-module``

Note that the version of errorprone used by the IDEA plugin might be older than
the one configured in the `pom.xml` and you might need to disable some checks
that are not yet supported by that older version. When in doubt, always check
with the full Maven build (``./mvnw clean install -DskipTests -Perrorprone-compiler``).

### Language injection in IDE

In order to enable language injection inside Intellij IDEA, some code elements
can be annotated with the `@org.intellij.lang.annotations.Language` annotation.
To make it useful, we recommend:

- Set the project-wide SQL dialect in ``Languages & Frameworks | SQL Dialects``
  "Generic SQL" is a decent choice here,
- Disable inspection ``SQL | No data source configured``,
- Optionally disable inspection ``Language injection | Language mismatch``.

See the [code style rules](CODE_STYLE.md#annotate-embedded-languages-with-language)
for where to use the annotation.

## Building docs

Information about writing and building the documentation can be found in
the [docs module](../docs).

## Building the Web UI

The Trino Web UI is a React and Vite project located in
`core/trino-web-ui/src/main/resources/webapp`. You must have
[Bun](https://bun.sh/docs/installation) installed to execute these
commands. (Maven builds download Bun automatically, so a local install is only
needed to run these commands by hand.) Install dependencies with:

    cd core/trino-web-ui/src/main/resources/webapp
    bun install

For fast local development, run the `WebUiQueryRunner` class. This starts a
minimal Trino development server configured with the Web UI. Then start the Vite
development server:

    bun run dev

Open `http://localhost:5173/ui` in your browser. The Vite development server
provides Hot Module Replacement for quick iteration. By default, requests to
`/ui/auth` and `/ui/api` are proxied to `http://127.0.0.1:8080/`. To use a
different backend, update `VITE_BASE_URL` in
`core/trino-web-ui/src/main/resources/webapp/.env.development`.

To build the Web UI locally, run:

    bun run build

To run frontend checks, run:

    bun run check

Maven builds package the Web UI automatically, and Maven verification runs the
frontend checks.

## Releases

Trino aims for frequent releases, generally once per week. This is a goal but
not a guarantee, as critical bugs may lead to a release being pushed back or
require an extra emergency release to patch the issue.

At the start of each release cycle, a release notes pull request (PR) is started
and maintained throughout the week, tracking all merged PRs to ensure every
change is properly documented and noted.

The PR uses the [release note template](../docs/release-template.md) and follows
the [release notes
guidelines](https://trino.io/development/process#release-note) to use and
improve the proposed release note entries from the merged PRs. When necessary,
documentation and clarification for the release notes entries is requested from
the merging maintainer and the contributor.

See [the release notes for
455](https://github.com/trinodb/trino/pull/23096) as an example.

Once it is time to release, the release notes PR is merged and the process is
kicked off. A code freeze is announced on the Trino Slack in the #releases
channel, and then a maintainer utilizes the [release
scripts](https://github.com/trinodb/release-scripts) to update Trino to the next
version.
