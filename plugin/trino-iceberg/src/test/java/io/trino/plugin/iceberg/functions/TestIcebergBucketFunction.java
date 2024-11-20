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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.type.IntegerType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergBucketFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(IcebergBucketFunction.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testIntegerTypes()
    {
        assertThat(assertions.function("bucket", "198765432", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(15);

        assertThat(assertions.function("bucket", "1987654329876", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(15);

        assertThat(assertions.function("bucket", "CAST(null as INTEGER)", "16"))
                .isNull();
    }

    @Test
    void testDecimalType()
    {
        assertThat(assertions.function("bucket", "DECIMAL '36.654'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(1);

        assertThat(assertions.function("bucket", "36.654", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(1);

        assertThat(assertions.function("bucket", "99099.9876", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(15);

        assertThat(assertions.function("bucket", "1110000000000000000000000.0", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(10);

        assertThat(assertions.function("bucket", "CAST(null as DECIMAL(5, 2))", "16"))
                .isNull();

        assertThat(assertions.function("bucket", "CAST(null as DECIMAL(24, 2))", "16"))
                .isNull();
    }

    @Test
    void testVarcharType()
    {
        assertThat(assertions.function("bucket", "'padraig'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(1);

        assertThat(assertions.function("bucket", "'padraig'", "32"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(17);

        assertThat(assertions.function("bucket", "CAST(null AS VARCHAR)", "16"))
                .isNull();
    }

    @Test
    void testDateType()
    {
        assertThat(assertions.function("bucket", "DATE '2024-11-19'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(11);

        assertThat(assertions.function("bucket", "DATE '2024-01-01'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(11);

        assertThat(assertions.function("bucket", "DATE '2023-11-01'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(9);

        assertThat(assertions.function("bucket", "CAST(null as DATE)", "16"))
                .isNull();
    }

    @Test
    void testTimestampType()
    {
        assertThat(assertions.function("bucket", "TIMESTAMP '1970-01-30 16:00:00'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(5);

        assertThat(assertions.function("bucket", "CAST(null as TIMESTAMP)", "16"))
                .isNull();
    }

    @Test
    void testTimestampWithTimeZoneType()
    {
        assertThat(assertions.function("bucket", "TIMESTAMP '1988-04-08 14:15:16 +02:09'", "16"))
                .hasType(IntegerType.INTEGER)
                .isEqualTo(0);

        assertThat(assertions.function("bucket", "CAST(null as TIMESTAMP WITH TIME ZONE)", "16"))
                .isNull();
    }

    @Test
    void testInvalidArguments()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("bucket", "'abc'")
                .evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
        assertTrinoExceptionThrownBy(() -> assertions.function("bucket", "'abc'", "'abc'", "'abc'")
                .evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }
}
