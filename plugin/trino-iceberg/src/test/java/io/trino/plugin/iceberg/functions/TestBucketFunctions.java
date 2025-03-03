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
package io.trino.plugin.iceberg.functions;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.sql.SqlPath;
import io.trino.sql.query.QueryAssertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestBucketFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions(testSessionBuilder()
                .setPath(SqlPath.buildPath("iceberg.system", Optional.empty()))
                .build());
        assertions.addPlugin(new IcebergPlugin());
        assertions.getQueryRunner().createCatalog("iceberg", "iceberg", ImmutableMap.of("hive.metastore.uri", "thrift://example.net:9083"));
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testTinyint()
    {
        assertBucket("TINYINT '-128'", 16, 2);
        assertBucket("TINYINT '127'", 16, 5);
        assertBucket("CAST(NULL AS TINYINT)", 16, null);
    }

    @Test
    void testSmallint()
    {
        assertBucket("SMALLINT '-32768'", 16, 1);
        assertBucket("SMALLINT '32767'", 16, 5);
        assertBucket("CAST(NULL AS SMALLINT)", 16, null);
    }

    @Test
    void testInteger()
    {
        assertBucket("1", 1, 0);
        assertBucket("198765432", 16, 15);
        assertBucket("1987654329876", 16, 15);
        assertBucket("CAST(NULL AS INTEGER)", 16, null);
    }

    @Test
    void testBigint()
    {
        assertBucket("-9223372036854775808", 16, 5);
        assertBucket("9223372036854775807", 16, 15);
        assertBucket("CAST(NULL AS BIGINT)", 16, null);
    }

    @Test
    void testDecimal()
    {
        assertBucket("DECIMAL '36.654'", 16, 1);
        assertBucket("36.654", 16, 1);
        assertBucket("99099.9876", 16, 15);
        assertBucket("1110000000000000000000000.0", 16, 10);
        assertBucket("CAST(NULL AS DECIMAL(5, 2))", 16, null);
        assertBucket("CAST(NULL AS DECIMAL(24, 2))", 16, null);
    }

    @Test
    void testVarchar()
    {
        assertBucket("'trino'", 16, 10);
        assertBucket("'iceberg'", 16, 9);
        assertBucket("CAST(NULL AS VARCHAR)", 16, null);
    }

    @Test
    void testVarbinary()
    {
        assertBucket("x'65683F'", 16, 11);
        assertBucket("CAST(NULL AS VARBINARY)", 16, null);
    }

    @Test
    void testDate()
    {
        assertBucket("DATE '0001-01-01'", 16, 9);
        assertBucket("DATE '2024-11-19'", 16, 11);
        assertBucket("DATE '9999-12-31'", 16, 9);
        assertBucket("CAST(NULL AS DATE)", 16, null);
    }

    @Test
    void testTimestamp()
    {
        assertBucket("TIMESTAMP '0001-01-01 00:00:00'", 1, 0);
        assertBucket("TIMESTAMP '1970-01-30 16:00:00'", 1, 0);
        assertBucket("TIMESTAMP '1970-01-30 16:00:00'", 16, 13);
        assertBucket("TIMESTAMP '1970-01-30 16:00:00.123456789012'", 16, 7);
        assertBucket("TIMESTAMP '1970-01-30 16:00:00.123456999999'", 16, 7);
        assertBucket("TIMESTAMP '9999-12-31 23:59:59.123456'", 16, 14);
        assertBucket("CAST(NULL AS TIMESTAMP)", 16, null);
        assertBucket("CAST(NULL AS TIMESTAMP(0))", 16, null);
        assertBucket("CAST(NULL AS TIMESTAMP(12))", 16, null);
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertBucket("TIMESTAMP '0001-01-01 00:00:00 +02:09'", 1, 0);
        assertBucket("TIMESTAMP '1988-04-08 14:15:16 +02:09'", 1, 0);
        assertBucket("TIMESTAMP '1988-04-08 14:15:16 +02:09'", 16, 0);
        assertBucket("TIMESTAMP '2025-01-01 00:00:00.123 +01:00'", 16, 10);
        assertBucket("TIMESTAMP '9999-12-31 23:59:59.123456 +00:00'", 16, 14);
        assertBucket("CAST(NULL AS TIMESTAMP WITH TIME ZONE)", 16, null);
        assertBucket("CAST(NULL AS TIMESTAMP(0) WITH TIME ZONE)", 16, null);
        assertBucket("CAST(NULL AS TIMESTAMP(12) WITH TIME ZONE)", 16, null);
    }

    @Test
    void testInvalidArguments()
    {
        assertInvalidType("true", "boolean");
        assertInvalidType("REAL '1.23'", "real");
        assertInvalidType("DOUBLE '1.23'", "double");
        assertInvalidType("CHAR 'abc'", "char(3)");
        assertInvalidType("uuid()", "uuid");
        assertInvalidType("ROW(1)", "row(integer)");
        assertInvalidType("ARRAY[1]", "array(integer)");
        assertInvalidType("MAP(ARRAY[1], ARRAY[2])", "map(integer, integer)");

        assertTrinoExceptionThrownBy(() -> assertions.function("bucket", "'abc'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
        assertTrinoExceptionThrownBy(() -> assertions.function("bucket", "'abc'", "'abc'", "'abc'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    private void assertBucket(@Language("SQL") String input, int bucketCount, Integer expectedBucket)
    {
        assertThat(assertions.function("bucket", input, Integer.toString(bucketCount)))
                .hasType(INTEGER)
                .isEqualTo(expectedBucket);
    }

    private void assertInvalidType(@Language("SQL")String input, String type)
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("bucket", input, "1").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Unsupported type: " + type);
    }
}
