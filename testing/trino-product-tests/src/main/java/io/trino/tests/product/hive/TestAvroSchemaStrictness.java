/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.hive;

import com.google.inject.Inject;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.tests.product.TestGroups.AVRO;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;
import static java.sql.JDBCType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Avro 1.8.2 was not strict about schema validation, causing the more-strict
 * Avro 1.9.2 to cause failures with some schemas that previously worked. This
 * was resolved in Avro 1.10.2's reader.
 *
 * <p>This class tests that Trino can read some data written with invalid
 * schemas by Avro 1.8.2.
 */
public class TestAvroSchemaStrictness
        extends ProductTest
{
    private static final Path ILLEGAL_UNION_DEFAULT_SCHEMA = Path.of("/docker/presto-product-tests/avro/invalid_default.avsc");
    /**
     *  The data in the avro data file was generated from the following JSON data using avro-tools.
     *  <pre>
     *  {"valid": {"string": "valid"}, "invalid": {"string": "invalid"}}
     *  {"valid": null, "invalid": null}
     *  </pre>
     *
     *  The command used:
     *  <pre>{@code
     *  java.jar avro-tools-1.8.2.jar fromjson --schema-file "$SCHEMA_FILE" data.json > invalid_default.avro
     *  }</pre>
     */
    private static final Path ILLEGAL_UNION_DEFAULT_DATA = Path.of("/docker/presto-product-tests/avro/invalid_default.avro");

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = AVRO)
    public void testInvalidUnionDefaults()
            throws IOException
    {
        String tableName = "invalid_union_default";
        String tablePath = format("/tmp/%s", tableName);
        String schemaPath = format("/tmp/%s.avsc", tableName);

        hdfsClient.createDirectory(tablePath);
        copyToHdfs(ILLEGAL_UNION_DEFAULT_DATA, Path.of(tablePath, "data.avro").toString());
        copyToHdfs(ILLEGAL_UNION_DEFAULT_SCHEMA, schemaPath);

        onTrino().executeQuery(
                format("CREATE TABLE %s (x int) with (\n"
                        + "format = 'AVRO',\n"
                        + "avro_schema_url = ?,\n"
                        + "external_location = ?\n"
                        + ")", tableName),
                param(VARCHAR, schemaPath),
                param(VARCHAR, tablePath));

        assertThat(onTrino().executeQuery("SELECT valid, invalid FROM " + tableName))
                .containsOnly(row("valid", "invalid"), row(null, null));
    }

    private String getHdfsPath(String firstSubPath, String... more)
    {
        return Path.of(firstSubPath, more).toString();
    }

    private void copyToHdfs(Path file, String hdfsPath)
            throws IOException
    {
        try (InputStream input = newInputStream(file)) {
            hdfsClient.saveFile(hdfsPath, input);
        }
    }
}
