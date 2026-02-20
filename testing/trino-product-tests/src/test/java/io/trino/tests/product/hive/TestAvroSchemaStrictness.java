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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static com.google.common.io.Resources.getResource;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;

/**
 * Avro 1.8.2 was not strict about schema validation, causing the more-strict
 * Avro 1.9.2 to cause failures with some schemas that previously worked. This
 * was resolved in Avro 1.10.2's reader.
 *
 * <p>This class tests that Trino can read some data written with invalid
 * schemas by Avro 1.8.2.
 *
 * <p>Ported from the Tempto-based TestAvroSchemaStrictness.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestAvroSchemaStrictness
{
    /**
     * The data in the avro data file was generated from the following JSON data using avro-tools.
     * <pre>
     * {"valid": {"string": "valid"}, "invalid": {"string": "invalid"}}
     * {"valid": null, "invalid": null}
     * </pre>
     *
     * The command used:
     * <pre>{@code
     * java.jar avro-tools-1.8.2.jar fromjson --schema-file "$SCHEMA_FILE" data.json > invalid_default.avro
     * }</pre>
     */
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInvalidUnionDefaults(HiveStorageFormatsEnvironment env)
            throws IOException
    {
        HdfsClient hdfsClient = env.createHdfsClient();

        String tableName = "invalid_union_default";
        String tablePath = format("/tmp/%s", tableName);
        String schemaPath = format("/tmp/%s.avsc", tableName);

        hdfsClient.createDirectory(tablePath);
        saveResourceOnHdfs(hdfsClient, "invalid_default.avro", tablePath + "/data.avro");
        saveResourceOnHdfs(hdfsClient, "invalid_default.avsc", schemaPath);

        try {
            env.executeTrino(format("CREATE TABLE hive.default.%s (x int) WITH (" +
                    "format = 'AVRO', " +
                    "avro_schema_url = '%s', " +
                    "external_location = '%s')",
                    tableName, schemaPath, tablePath));

            assertThat(env.executeTrino("SELECT valid, invalid FROM hive.default." + tableName))
                    .containsOnly(row("valid", "invalid"), row(null, null));
        }
        finally {
            env.executeTrino("DROP TABLE IF EXISTS hive.default." + tableName);
            hdfsClient.delete(tablePath);
            hdfsClient.delete(schemaPath);
        }
    }

    private void saveResourceOnHdfs(HdfsClient hdfsClient, String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Path.of("io/trino/tests/product/hive/data/avro/", resource).toString()).openStream()) {
            byte[] content = inputStream.readAllBytes();
            hdfsClient.saveFile(location, content);
        }
    }
}
