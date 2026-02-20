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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@ProductTest
@RequiresEnvironment(SparkIcebergRestEnvironment.class)
@TestGroup.IcebergRest
class TestIcebergSparkRestCompatibility
{
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @ParameterizedTest
    @MethodSource("variantStorageFormats")
    void testSparkReadsTrinoVariantData(StorageFormat storageFormat, SparkIcebergRestEnvironment env)
    {
        String tableName = toLowerCase(format("test_spark_reads_trino_variant_%s_%s", storageFormat, randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        // Spark does not yet support the full Iceberg VARIANT primitive set.
        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(id INT, v VARIANT) " +
                "WITH(format_version = 3, format = '" + storageFormat + "')");
        env.executeTrinoUpdate(
                """
                INSERT INTO %s VALUES
                    (1, CAST(NULL AS VARIANT)),
                    (2, CAST(JSON 'null' AS VARIANT)),
                    (3, CAST(true AS VARIANT)),
                    (4, CAST(TINYINT '1' AS VARIANT)),
                    (5, CAST(SMALLINT '1' AS VARIANT)),
                    (6, CAST(INTEGER '-2' AS VARIANT)),
                    (7, CAST(BIGINT '1234567890123' AS VARIANT)),
                    (8, CAST(REAL '1.5' AS VARIANT)),
                    (9, CAST(DOUBLE '2.5' AS VARIANT)),
                    (10, CAST(DECIMAL '123.45' AS VARIANT)),
                    (11, CAST('hello "variant"' AS VARIANT)),
                    (12, CAST(ARRAY[1, 2, 3] AS VARIANT)),
                    (13, CAST(MAP(ARRAY['a', 'b'], ARRAY[1, 2]) AS VARIANT)),
                    (14, CAST(CAST(ROW(42, 'x', true) AS ROW(id integer, vc varchar, flag boolean)) AS VARIANT))
                """.formatted(trinoTableName));

        QueryResult trinoResult = env.executeTrino("SELECT id, json_format(CAST(v AS JSON)) FROM " + trinoTableName + " ORDER BY id");
        QueryResult sparkResult = env.executeSpark("SELECT id, to_json(v) FROM " + sparkTableName + " ORDER BY id");
        assertResultsEqual(trinoResult, sparkResult);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("variantStorageFormats")
    void testTrinoReadsSparkVariantData(StorageFormat storageFormat, SparkIcebergRestEnvironment env)
    {
        String tableName = toLowerCase(format("test_trino_reads_spark_variant_%s_%s", storageFormat, randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        // Spark does not yet implement the full Iceberg VARIANT primitive set.
        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(id INT, v VARIANT) " +
                "USING ICEBERG TBLPROPERTIES ('format-version'='3', 'write.format.default'='" + storageFormat + "')");
        env.executeSparkUpdate(
                """
                INSERT INTO %s VALUES
                    (1, CAST(NULL AS VARIANT)),
                    (2, parse_json('null')),
                    (3, CAST(true AS VARIANT)),
                    (4, CAST(CAST(1 AS TINYINT) AS VARIANT)),
                    (5, CAST(CAST(1 AS SMALLINT) AS VARIANT)),
                    (6, CAST(CAST(-2 AS INT) AS VARIANT)),
                    (7, CAST(CAST(1234567890123 AS BIGINT) AS VARIANT)),
                    (8, CAST(CAST(1.5 AS FLOAT) AS VARIANT)),
                    (9, CAST(CAST(2.5 AS DOUBLE) AS VARIANT)),
                    (10, CAST(CAST(123.45 AS DECIMAL(5, 2)) AS VARIANT)),
                    (11, CAST('hello "variant"' AS VARIANT)),
                    (12, CAST(DATE '2021-07-24' AS VARIANT)),
                    (13, to_variant_object(array(1, 2, 3))),
                    (14, to_variant_object(named_struct('flag', true, 'id', CAST(42 AS TINYINT), 'vc', 'x')))
                """.formatted(sparkTableName));

        QueryResult sparkResult = env.executeSpark("SELECT id, to_json(v) FROM " + sparkTableName + " ORDER BY id");
        QueryResult trinoResult = env.executeTrino("SELECT id, json_format(CAST(v AS JSON)) FROM " + trinoTableName + " ORDER BY id");
        assertResultsEqual(sparkResult, trinoResult);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    static Stream<Arguments> variantStorageFormats()
    {
        return Stream.of(StorageFormat.AVRO, StorageFormat.PARQUET)
                .map(Arguments::of);
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        assertThat(first).containsExactlyInOrder(second.getRows());
        assertThat(second).containsExactlyInOrder(first.getRows());
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.%s.%s", SPARK_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private static String toLowerCase(String name)
    {
        return name.toLowerCase(ENGLISH);
    }

    enum StorageFormat
    {
        AVRO,
        PARQUET,
    }
}
