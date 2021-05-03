# Trino documentation

The `docs` module contains the reference documentation for Trino.

- [Writing and contributing](#writing-and-contributing)
- [Tools](#tools)
- [Faster build for authoring](#faster-build-for-authoring)
- [Default build](#default-build)
- [Viewing documentation](#viewing-documentation)
- [Versioning](#versioning)
- [Contribution requirements](#contribution-requirements)
- [Workflow](#workflow)
- [Videos](#videos)

## Writing and contributing

We welcome any contributions to the documentation. Contributions must [follow
the same process as code contributions](https://trino.io/development/) and
can be part of your code contributions or separate documentation improvements.

The documentation follows the Google developer documentation style guide for any
new documentation:

- [Google developer documentation style guide](https://developers.google.com/style)
- [Highlights](https://developers.google.com/style/highlights)
- [Word list](https://developers.google.com/style/word-list)
- [Style and tone](https://developers.google.com/style/tone)
- [Writing for a global audience](https://developers.google.com/style/translation)
- [Cross-references](https://developers.google.com/style/cross-references)
- [Present tense](https://developers.google.com/style/tense)

The Google guidelines include more material than listed here, and are used as a
guide that enable easy decision making about proposed doc changes. Changes to
existing documentation to follow these guidelines are underway.

Other useful resources:

- [Google Technical Writing Courses](https://developers.google.com/tech-writing)
- [RST cheatsheet](https://github.com/ralsina/rst-cheatsheet/blob/master/rst-cheatsheet.rst)

## Tools

Documentation source files can be found in [Restructured
Text](https://en.wikipedia.org/wiki/ReStructuredText) (`.rst`) format in
`src/main/sphinx` and sub-folders.

The engine used to create the documentation in HTML format is the Python-based
[Sphinx](https://www.sphinx-doc.org).

The [fast doc build option](#fast-doc-build-option) requires *only* a local
installation of [Docker Desktop on
Mac](https://docs.docker.com/docker-for-mac/install/) or [Docker Engine on
Linux](https://docs.docker.com/engine/install/). No other tools are required.

The default formal build of the docs is performed with Apache Maven, which requires an
installation of a Java Development Kit.

## Fast doc build option

For fast local build times when writing documentation, you can run the Sphinx
build directly. The build runs inside a Docker container and thus does not
require having anything installed locally other than Docker. You can run the
Sphinx build on a fresh clone of the project, with no prerequisite commands. For
example:

```bash
docs/build
```

Sphinx attempts to perform an incremental build, but this does not work
in all cases, such as after editing the CSS. You can force a full rebuild
by removing the ``target/html`` directory:

```bash
rm -rf docs/target/html
```

## Default build

The default build uses Apache Maven and Java as does the rest of the
Trino build. You only need to have built the current Trino version from the root.
That is, before building the docs the first time, run the following command:

```bash
./mvnw clean install -DskipTests
```

Subsequently, you can build the doc site using the Maven wrapper script:

```bash
./mvnw -pl docs clean install
```

If you have Maven installed and available on the path, you can use the `mvn` command
directly.

This also performs other checks, and is the authoritative way to build the
docs. However, using Maven is also somewhat slower than using Sphinx directly.

## Viewing documentation

However you build the docs, the generated HTML files can be found in the folder
`docs/target/html/`.

You can open the file `docs/target/html/index.html` in a web browser on
macOS with

```bash
open docs/target/html/index.html
```

or on Linux with

```bash
xdg-open docs/target/html/index.html
```

Or you can directly call your browser of choice with the same filename. For example, on Ubuntu
with Chromium:

```bash
chromium-browser docs/target/html/index.html
```

Alternatively, you can start a web server with that folder as root, such as with
the following Python command. You can then open
[http://localhost:4000](http://localhost:4000) in a web browser.

```bash
cd docs/target/html/
python3 -m http.server 4000
```

In order to see any changes from the source files in the HTML output, simply
re-run the ``build`` command and refresh the browser.

## Versioning

The version displayed in the resulting HTML is read by default from the top level Maven
`pom.xml` file `version` field.

To deploy a specific documentation set (such as a SNAPSHOT version) as the release
version you must override the pom version with the `TRINO_VERSION`
environment variable.

```bash
TRINO_VERSION=355 docs/build
```

If you work on the docs for more than one invocation, you can export the
variable and use it with Sphinx.

```bash
export TRINO_VERSION=354
docs/build
```

This is especially useful when deploying doc patches for a release where the
Maven pom has already moved to the next SNAPSHOT version.

## Contribution requirements


To contribute corrections or new explanations to the Trino documentation requires
only a willingness to help and submission of your [Contributor License
Agreement](https://github.com/trinodb/cla) (CLA).

## Workflow

The procedure to add a documentation contribution is the same as for [a code
contribution](https://trino.io/development/process.html).

* In the Trino project's [GitHub Issues
  list](https://github.com/trinodb/trino/issues), identify documentation issues
  by filtering on the [``docs``
  label](https://github.com/trinodb/trino/issues?q=is%3Aissue+is%3Aopen+label%3Adocs).

* If you want to help Trino documentation, but don't know where to start, look
  in the Issues list for both the [``docs`` and ``good first issue``
  labels](https://github.com/trinodb/trino/issues?q=is%3Aissue+is%3Aopen+label%3Adocs+label%3A%22good+first+issue%22).

* If the doc fix you have in mind does not yet have an issue, add one (which
  requires a signed CLA). Add the ``docs`` label to your new issue.

* You can discuss proposed doc changes in the #docs channel of the [Trino
  Slack](https://trino.io/slack.html).

* For a larger contribution, create a GitHub pull request as described
  in [GitHub
  documentation](https://docs.github.com/en/github/getting-started-with-github).
  In brief, this means:

  * [Create a fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) of the
    [trinodb/trino](https://github.com/trinodb/trino) repository.

  * Create a working branch in your fork.

  * Make your edits in your working branch and push them to your fork.

  * In a browser, open your fork in GitHub, which offers to submit a pull
    request for you.

## Videos

1. See [**Contributing to the Trino
   documentation**](https://www.youtube.com/watch?v=yseFM3ZI2ro) for a
   five-minute video introduction.

2. You might select a GitHub doc issue to work on that requires you to verify
   how Trino handles a situation, such as [adding
   documentation](https://github.com/trinodb/trino/issues/7660) for a SQL
   language function.

   In this case, the five-minute video [Learning Trino SQL with
   Docker](https://www.youtube.com/watch?v=y58sb9bW2mA) gives you a starting
   point for setting up a test system on your laptop.

