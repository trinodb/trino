# Trino documentation

The `docs` module contains the reference documentation for Trino.

- [Writing and contributing](#writing-and-contributing)
- [Tools](#tools)
- [Fast doc build option](#fast-doc-build-option)
- [Default build](#default-build)
- [Viewing documentation](#viewing-documentation)
- [Versioning](#versioning)
- [Style check](#style-check)
- [Contribution requirements](#contribution-requirements)
- [Workflow](#workflow)
- [Videos](#videos)
- [Docker container](#docker-container)

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

As a specific style note, because different readers may perceive the phrases "a
SQL" or "an SQL" to be incorrect depending on how they pronounce SQL, aim to
avoid use of "a/an SQL" in Trino documentation. Try to reword, re-order, or
adjust writing so that it is not necessary. If there is absolutely no way around
it, default to using "a SQL."

Other useful resources:

- [Style check](#style-check)
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

## Style check

The project contains a configured setup for [Vale](https://vale.sh) and the
Google developer documentation style. Vale is a command-line tool to check for
editorial style issues of a document or a set of documents.

Install vale with brew on macOS or follow the instructions on the website.

```
brew install vale
```

The `docs` folder contains the necessary configuration to use vale for any
document in the repository:

* `.vale` directory with Google style setup
* `.vale/Vocab/Base/accept.txt` file for additional approved words and spelling
* `.vale.ini` configuration file configured for rst and md files

With this setup you can validate an individual file from the root by specifying
the path:

```
vale src/main/sphinx/overview/sep-ui.rst
```

You can also use directory paths and all files within.

Treat all output from vale as another help towards better docs. Fixing any
issues is not required, but can help with learning more about the [Google style
guide](https://developers.google.com/style) that we try to follow.

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
   documentation](https://github.com/trinodb/trino/issues/7660) for SQL
   functions.

   In this case, the five-minute video [Learning Trino SQL with
   Docker](https://www.youtube.com/watch?v=y58sb9bW2mA) gives you a starting
   point for setting up a test system on your laptop.

## Docker container

The build of the docs uses a Docker container that includes Sphinx and the
required libraries. The container is referenced in the `SPHINX_IMAGE` variable
in the `build` script.

The specific details for the container are available in `Dockerfile`, and
`requirements.in`. The file `requirements.txt` must be updated after any changes
to `requirements.in`.

The container must be published to the GitHub container registry at ghcr.io with
the necessary access credentials and the following command, after modification
of the version tag `xxx` to the new desired value as used in the `build` script:

```
docker buildx build docs --platform=linux/arm64,linux/amd64 --tag ghcr.io/trinodb/build/sphinx:xxx --provenance=false --push
```

Note that the version must be updated and the command automatically also
publishes the container with support for arm64 and amd64 processors. This is
necessary so the build performs well on both hardware platforms.

After the container is published, you can update the `build` script and merge
the related pull request.

Example PRs:

* https://github.com/trinodb/trino/pull/17778
* https://github.com/trinodb/trino/pull/13225


