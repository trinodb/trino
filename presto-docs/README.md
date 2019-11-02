# presto-docs - Presto Documentation

The presto-docs module contains the reference documentation for Presto.

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
Subsequently you can build the site using the Maven wrapper script.

```bash
./mvnw -pl presto-docs clean install
```

or

```bash
cd presto-docs
../mvnw clean install
```

If you have Maven installed and available on the path, you can use `mvn`
directly.

This also performs other checks and it is the authoritative way to build the
docs, however it is also considerably slower than using Sphinx directly. In some
circumstances it can also hide errors that do show up with native Sphinx usage.

## Faster Build for Authoring

For faster local build times when writing documentation, we suggest to use the
Sphinx and the included `make` script.

Sphinx installation instructions for various operating systems and packaging
systems are [available on the Sphinx site](https://www.sphinx-doc.org/en/master/usage/installation.html).

In addition you need `make` and Python.

With the tools installed and available on the PATH, you can build the docs
easily with make:

```bash
make -C presto-docs clean html
```

or

```bash
cd presto-docs
make clean html
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

## Known Issues

- Older Sphinx versions do not support the `-j auto` SPHINXOPTS in the makefile.
  You can delete the option or upgrade Sphinx. The correct version of sphinx is
  embedded in the Maven plugin used for the default build.
- Formats like `man` and others beyond the default `html` might have formatting
  and content issues and are not actively maintained.
- Different installation methods for Sphinx result in different versions, and
  hence in sometimes different problems. Especially when also using
  sphinx-autobuild we recommend using the `pip`-based installation.

