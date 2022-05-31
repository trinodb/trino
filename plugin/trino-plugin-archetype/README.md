# trino-plugin-archetype

Maven archetype to create new Trino plugins. It provides:
* source code to build a plugin that defines one example table called `single_row`, with 3 columns: `id`, `type` and `name`, that always returns a single row with values `x`, `default-value` (from the config class) and `my-name`
* integration tests for this plugin, that run it and performs a `SELECT` query
* a basic `README.md`
* a `LICENSE` file with the Apache License 2.0
* Maven wrapper

## Usage

Run the following, which will create a new directory with the plugin project:

```bash
mvn archetype:generate \
  -DarchetypeGroupId=io.trino \
  -DarchetypeArtifactId=trino-plugin-archetype \
  -DarchetypeVersion=383 \
  -DgroupId=<my.groupid> \
  -DartifactId=<my.artifactId>
  -DclassPrefix=<Name>
  -DconnectorName=<name>
```
