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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * JUnit 5 port of Delta Lake drop table compatibility tests from TestDeltaLakeDropTableCompatibility.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 * Tests marked only with DELTA_LAKE_DATABRICKS are not included.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeDropTableCompatibility
{
    enum Engine
    {
        TRINO,
        DELTA
    }

    static Stream<Arguments> engineConfigurations()
    {
        return Stream.of(
                Arguments.of(Engine.TRINO, Engine.TRINO, true),
                Arguments.of(Engine.TRINO, Engine.TRINO, false),
                Arguments.of(Engine.TRINO, Engine.DELTA, true),
                Arguments.of(Engine.TRINO, Engine.DELTA, false),
                Arguments.of(Engine.DELTA, Engine.TRINO, true),
                Arguments.of(Engine.DELTA, Engine.TRINO, false),
                Arguments.of(Engine.DELTA, Engine.DELTA, true),
                Arguments.of(Engine.DELTA, Engine.DELTA, false));
    }

    @ParameterizedTest
    @MethodSource("engineConfigurations")
    void testDropTable(Engine creator, Engine dropper, boolean explicitLocation, DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String schemaName = "test_schema_with_location_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/databricks-compatibility-test-%s", bucketName, schemaName);
        String tableName = explicitLocation ? "test_external_table" : "test_managed_table";
        Optional<String> tableLocation = explicitLocation
                ? Optional.of(format("s3://%s/databricks-compatibility-test-%s/%s", bucketName, schemaName, tableName))
                : Optional.empty();

        switch (creator) {
            case TRINO -> env.executeTrinoUpdate(format("CREATE SCHEMA delta.%s WITH (location = '%s')", schemaName, schemaLocation));
            case DELTA -> env.executeSparkUpdate(format("CREATE SCHEMA %s LOCATION \"%s\"", schemaName, schemaLocation));
            default -> throw new UnsupportedOperationException("Unsupported engine: " + creator);
        }
        try {
            env.executeTrinoUpdate("USE delta." + schemaName);
            switch (creator) {
                case TRINO -> env.executeTrinoUpdate(format(
                        "CREATE TABLE delta.%s.%s (a, b) %s AS VALUES (1, 2), (2, 3), (3, 4)",
                        schemaName,
                        tableName,
                        tableLocation.map(location -> "WITH (location = '" + location + "')").orElse("")));
                case DELTA -> env.executeSparkUpdate(format(
                        "CREATE TABLE %s.%s USING DELTA %s AS VALUES (1, 2), (2, 3), (3, 4)",
                        schemaName,
                        tableName,
                        tableLocation.map(location -> "LOCATION \"" + location + "\"").orElse("")));
                default -> throw new UnsupportedOperationException("Unsupported engine: " + creator);
            }

            String objectPrefix = "databricks-compatibility-test-" + schemaName + "/" + tableName;

            try (MinioClient minioClient = env.createMinioClient()) {
                List<String> tableFiles = minioClient.listObjects(bucketName, objectPrefix);
                assertThat(tableFiles).isNotEmpty();

                switch (dropper) {
                    case DELTA -> env.executeSparkUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
                    case TRINO -> env.executeTrinoUpdate("DROP TABLE delta." + schemaName + "." + tableName);
                    default -> throw new UnsupportedOperationException("Unsupported engine: " + dropper);
                }

                tableFiles = minioClient.listObjects(bucketName, objectPrefix);
                if (explicitLocation) {
                    assertThat(tableFiles).isNotEmpty();
                }
                else {
                    assertThat(tableFiles).isEmpty();
                }
            }
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            env.executeSparkUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
