# presto-docs - Presto Documentation

The presto-docs module contains the reference documentation for Presto.

## Tools

The engine used to create the documentation in HTML format is the Python-based
[Sphinx](https://www.sphinx-doc.org). Installation instructions for various OS
are [available on the Sphinx site](https://www.sphinx-doc.org/en/master/usage/installation.html).

Documentation source files can be found in [Restructured
Text](https://en.wikipedia.org/wiki/ReStructuredText) (`.rst`) format in
`src/main/sphinx` and sub-folders.

In addition, `make` is required

## Build and Read

With the tools installed and available on the PATH, you can build the docs
easily with make:

```bash
cd presto/presto-docs
make clean html
```

Alternatively you can keep using Apache Maven and Java like for the rest of the
Presto build. You just need to have built the current version from the root.
Subsequently you can build the site:

```bash
cd presto/presto-docs
mvn clean install
```

It calls the make command and performs other checks, so you still need the tools
installed.

However you built the docs, the output HTML files can be found in the folder
`presto-docs/target/html/`.

You can open the file `presto-docs/target/html/index.html` in a web browser on
macOS with

```bash
open presto-docs/target/html/index.html
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

## Known Issues

- Older Sphinx versions do not support the `-j auto` SPHINXOPTS in the makefile.
  You can delete the option or upgrade Sphinx.
- Formats like `man` and others beyond the default `html` might have formatting
  and content issues and are not actively maintained.
