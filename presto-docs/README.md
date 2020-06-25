# presto-docs - Presto Documentation

The presto-docs module contains the reference documentation for Presto.

- [Writing and contributing](#writing-and-contributing)
- [Tools](#tools)
- [Default Build](#default-build)
- [Faster Build for Authoring](#faster-build-for-authoring)
- [Viewing Documentation](#viewing-documentation)
- [Using sphinx-autobuild](#using-sphinx-autobuild)
- [Versioning](#versioning)
- [Known Issues](#known-issues)

## Writing and contributing

We welcome any contributions to the documentation. Contributions need to [follow
the same process as code contributions](https://prestosql.io/development/) and
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

The guidelines include much more material and are used as a guide and enable
easy decision making. Existing documentation upgrades to follow the guidelines
are ongoing.

Other useful resources:

- [Google Technical Writing Courses](https://developers.google.com/tech-writing)
- [RST cheatsheet](https://github.com/ralsina/rst-cheatsheet/blob/master/rst-cheatsheet.rst)

## Tools

The default build of the docs is performed with Apache Maven.

Documentation source files can be found in [Restructured
Text](https://en.wikipedia.org/wiki/ReStructuredText) (`.rst`) format in
`src/main/sphinx` and sub-folders.

The engine used to create the documentation in HTML format is the Python-based
[Sphinx](https://www.sphinx-doc.org).

## Default Build

The default build is using Apache Maven and Java like for the rest of the
Presto build. You just need to have built the current version from the root.
Subsequently, you can build the site using the Maven wrapper script.

```bash
./mvnw -pl presto-docs clean install
```

If you have Maven installed and available on the path, you can use `mvn`
directly.

This also performs other checks, and it is the authoritative way to build the
docs, however it is somewhat also slower than using Sphinx directly.

## Faster Build for Authoring

For faster local build times when writing documentation, you can run the
Sphinx build directly. The build runs inside a Docker container and thus
does not require having anything installed locally (except for Docker):

```bash
presto-docs/build
```

Sphinx will attempt to perform an incremental build, but it does not work
in all cases, such as after editing the CSS. You can force a full rebuild
by doing a Maven clean first:

```bash
./mvnw -pl presto-docs clean
```

## Viewing Documentation

However you built the docs, the output HTML files can be found in the folder
`presto-docs/target/html/`.

You can open the file `presto-docs/target/html/index.html` in a web browser on
macOS with

```bash
open presto-docs/target/html/index.html
```

or on Linux with

```bash
xdg-open presto-docs/target/html/index.html
```

Or you can directly call your browser of choice with the filename e.g on Ubuntu
with Chromium:

```bash
chromium-browser presto-docs/target/html/index.html
```

Alternatively, you can start a web server with that folder as root, e.g. again
with Python and then open [http://localhost:4000](http://localhost:4000) in a
web browser.

```bash
cd presto-docs/target/html/
python3 -m http.server 4000
```

In order to see any changes from the source files in the HTML output, simply
re-run the make command and refresh the browser.

## Using sphinx-autobuild

The optional setup of using
[sphinx-autobuild](https://pypi.org/project/sphinx-autobuild/) allows you to
have a running server with the docs and get incremental updates after saving any
changes. This is the fastest and best way to work on the documentation.

To use it, simply install sphinx-autobuild, and then run

```bash
make clean livehtml
```

From now on the docs are available at
[http://localhost:8000](http://localhost:8000).

## Versioning

The version displayed in the resulting HTML is read from the top level Maven
`pom.xml` file `version` field, by default.

To deploy a specific documentation set (e.g. a SNAPSHOT version) as release
version you have to override the pom version with the `PRESTO_VERSION`
environment variable.

```bash
PRESTO_VERSION=327 make clean html
```

If you work on the docs for more than one invocation, you can export the
variable and use it with sphinx as well as sphinx-autobuild.

```bash
export PRESTO_VERSION=327
make clean html
```

This is especially useful when deploying doc patches for a release where the
Maven pom has already moved to the next SNAPSHOT version.

## Known Issues

- Older Sphinx versions do not support the `-j auto` SPHINXOPTS in the makefile.
  You can delete the option or upgrade Sphinx. The correct version of sphinx is
  embedded in the Maven plugin used for the default build.
- Formats like `man` and others beyond the default `html` might have formatting
  and content issues and are not actively maintained.
- Different installation methods for Sphinx result in different versions, and
  hence in sometimes different problems. Especially when also using
  sphinx-autobuild we recommend using the `pip`-based installation.
- Sphinx 2.x+ fails due to requiring parallel write support, which our sitemap extension
  does not support. We recommend installing an older version by running
  `pip3 install sphinx==1.8.2`. Alternatively, you clear `SPHINXOPTS` when running
  Sphinx by using `make SPHINXOPTS="" clean html`, but this may result in other
  compatibility issues or differences from the output produced by the Maven plugin.
