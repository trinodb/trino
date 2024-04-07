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
package io.trino.type;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRescalingDecimalDivision
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("default")
                .build(), server -> server.addProperty("decimal-rescaling-enabled", "true"));

        assertions = new QueryAssertions(queryRunner);

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestMapOperators.class)
                .build());
    }

    @Test
    public void testDivide()
    {
        // short short -> short
        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.333333", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-0.333333", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.333333", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '0.1'"))
                .isEqualTo(decimal("10.000000", createDecimalType(8, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '9.0'"))
                .isEqualTo(decimal("00.111111", createDecimalType(8, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '500.00'", "DECIMAL '0.1'"))
                .isEqualTo(decimal("5000.000000", createDecimalType(10, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0333.333333", createDecimalType(10, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("00333.333333", createDecimalType(11, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal("-00333.333333", createDecimalType(11, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-100.00'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("-00333.333333", createDecimalType(11, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0666.666667", createDecimalType(10, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00000'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0666.6666667", createDecimalType(11, 7)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00000'", "DECIMAL '-0.3'"))
                .isEqualTo(decimal("-0666.6666667", createDecimalType(11, 7)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-200.00000'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("-0666.6666667", createDecimalType(11, 7)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999'", "DECIMAL '1'"))
                .isEqualTo(decimal("999999999999999999.000000", createDecimalType(24, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '000000000000000003'"))
                .isEqualTo(decimal("3.000000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("3.000000", createDecimalType(8, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999'", "DECIMAL '500000000000000000'"))
                .isEqualTo(decimal("1.9999999999999999980", createDecimalType(37, 19)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("0.0000000000000000010", createDecimalType(20, 19)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("-0.0000000000000000010", createDecimalType(20, 19)));

        // round
        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '5'"))
                .isEqualTo(decimal("1.800000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '7'", "DECIMAL '5'"))
                .isEqualTo(decimal("1.400000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9'", "DECIMAL '5'"))
                .isEqualTo(decimal("-1.800000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-7'", "DECIMAL '5'"))
                .isEqualTo(decimal("-1.400000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9'", "DECIMAL '-5'"))
                .isEqualTo(decimal("1.800000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-7'", "DECIMAL '-5'"))
                .isEqualTo(decimal("1.400000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '-5'"))
                .isEqualTo(decimal("-1.800000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '7'", "DECIMAL '-5'"))
                .isEqualTo(decimal("-1.400000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '2'"))
                .isEqualTo(decimal("-0.500000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '-2'"))
                .isEqualTo(decimal("-0.500000", createDecimalType(7, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '3'"))
                .isEqualTo(decimal("-0.333333", createDecimalType(7, 6)));

        // short short -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '10'", "DECIMAL '.000000001'"))
                .isEqualTo(decimal("10000000000.0000000000", createDecimalType(21, 10)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-10'", "DECIMAL '.000000001'"))
                .isEqualTo(decimal("-10000000000.0000000000", createDecimalType(21, 10)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '10'", "DECIMAL '-.000000001'"))
                .isEqualTo(decimal("-10000000000.0000000000", createDecimalType(21, 10)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-10'", "DECIMAL '-.000000001'"))
                .isEqualTo(decimal("10000000000.0000000000", createDecimalType(21, 10)));

        // long short -> long
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '200000000000000000000000000000000000'", "DECIMAL '0.30'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '200000000000000000000000000000000000'", "DECIMAL '-0.30'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertThat(assertions.operator(DIVIDE, "DECIMAL '20000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("66666666666666666666666666666666.666667", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '20000000000000000000000000000000'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal("-66666666666666666666666666666666.666667", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-.20000000000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("-0.666666666666666666666666666666666667", createDecimalType(38, 36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-.20000000000000000000000000000000000000'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal("0.666666666666666666666666666666666667", createDecimalType(38, 36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '.20000000000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("0.666666666666666666666666666666666667", createDecimalType(38, 36)));

        // round
        assertThat(assertions.operator(DIVIDE, "DECIMAL '50000000000000000000000000000075'", "DECIMAL '50'"))
                .isEqualTo(decimal("1000000000000000000000000000001.500000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '50000000000000000000000000000070'", "DECIMAL '50'"))
                .isEqualTo(decimal("1000000000000000000000000000001.400000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-50000000000000000000000000000075'", "DECIMAL '50'"))
                .isEqualTo(decimal("-1000000000000000000000000000001.500000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-50000000000000000000000000000070'", "DECIMAL '50'"))
                .isEqualTo(decimal("-1000000000000000000000000000001.400000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '50000000000000000000000000000075'", "DECIMAL '-50'"))
                .isEqualTo(decimal("-1000000000000000000000000000001.500000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '50000000000000000000000000000070'", "DECIMAL '-50'"))
                .isEqualTo(decimal("-1000000000000000000000000000001.400000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-50000000000000000000000000000075'", "DECIMAL '-50'"))
                .isEqualTo(decimal("1000000000000000000000000000001.500000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-50000000000000000000000000000070'", "DECIMAL '-50'"))
                .isEqualTo(decimal("1000000000000000000000000000001.400000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "CAST(-1 AS DECIMAL(19,0))", "DECIMAL '2'"))
                .isEqualTo(decimal("-0.500000", createDecimalType(25, 6)));

        assertThat(assertions.operator(DIVIDE, "CAST(1 AS DECIMAL(19,0))", "DECIMAL '-2'"))
                .isEqualTo(decimal("-0.500000", createDecimalType(25, 6)));

        assertThat(assertions.operator(DIVIDE, "CAST(-1 AS DECIMAL(19,0))", "DECIMAL '3'"))
                .isEqualTo(decimal("-0.333333", createDecimalType(25, 6)));

        // short long -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.1'", "DECIMAL '.0000000000000000001'"))
                .isEqualTo(decimal("1000000000000000000.0000000000000000000", createDecimalType(38, 19)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.1'", "DECIMAL '.0000000000000000001'"))
                .isEqualTo(decimal("-1000000000000000000.0000000000000000000", createDecimalType(38, 19)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.1'", "DECIMAL '-.0000000000000000001'"))
                .isEqualTo(decimal("-1000000000000000000.0000000000000000000", createDecimalType(38, 19)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.1'", "DECIMAL '-.0000000000000000001'"))
                .isEqualTo(decimal("1000000000000000000.0000000000000000000", createDecimalType(38, 19)));

        // short long -> short
        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '000000000000000003.0'"))
                .isEqualTo(decimal("3.000000", createDecimalType(8, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        // long long -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999999999999999999999'", "DECIMAL '111111111111111111111111111111111111'"))
                .isEqualTo(decimal("9.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-999999999999999999999999999999999999'", "DECIMAL '111111111111111111111111111111111111'"))
                .isEqualTo(decimal("-9.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999999999999999999999'", "DECIMAL '-111111111111111111111111111111111111'"))
                .isEqualTo(decimal("-9.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-999999999999999999999999999999999999'", "DECIMAL '-111111111111111111111111111111111111'"))
                .isEqualTo(decimal("9.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '111111111111111111111111111111111111'", "DECIMAL '999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.111111", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-111111111111111111111111111111111111'", "DECIMAL '999999999999999999999999999999999999'"))
                .isEqualTo(decimal("-0.111111", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '111111111111111111111111111111111111'", "DECIMAL '-999999999999999999999999999999999999'"))
                .isEqualTo(decimal("-0.111111", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-111111111111111111111111111111111111'", "DECIMAL '-999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0.111111", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999999999999999999998'", "DECIMAL '999999999999999999999999999999999999'"))
                .isEqualTo(decimal("1.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9999999999999999999999999999999999.8'", "DECIMAL '9999999999999999999999999999999999.9'"))
                .isEqualTo(decimal("1.000000", createDecimalType(38, 6)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9999999999999999999999.9'", "DECIMAL '1111111111111111111111.100'"))
                .isEqualTo(decimal("9.0000000000000", createDecimalType(38, 13)));

        assertThat(assertions.operator(DIVIDE, "CAST('1635619.3155' AS DECIMAL(32,4))", "CAST('47497517.7405' AS DECIMAL(32,4))"))
                .isEqualTo(decimal("0.034436", createDecimalType(38, 6)));

        // runtime overflow
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '.12345678901234567890123456789012345678'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '.12345678901234567890123456789012345678'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '1.000000000000000000000000000000000000'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '1.000000000000000000000000000000000000'", "DECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.operator(DIVIDE, "CAST(1000 AS DECIMAL(38,8))", "CAST(25 AS DECIMAL(38,8))"))
                .isEqualTo(decimal("40.000000", createDecimalType(38, 6)));
    }

    @Test
    public void testDivideDecimalBigint()
    {
        // bigint / decimal
        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("3.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("-3.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '-3.0'"))
                .isEqualTo(decimal("-3.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '-3.0'"))
                .isEqualTo(decimal("3.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '000000000000000003.0'"))
                .isEqualTo(decimal("3.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '18'", "DECIMAL '0.01'"))
                .isEqualTo(decimal("1800.000000", createDecimalType(27, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '00000000000000000.1'"))
                .isEqualTo(decimal("90.000000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '300.0'"))
                .isEqualTo(decimal("0.030000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '300.0'"))
                .isEqualTo(decimal("-0.030000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '-300.0'"))
                .isEqualTo(decimal("-0.030000", createDecimalType(26, 6)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '-300.0'"))
                .isEqualTo(decimal("0.030000", createDecimalType(26, 6)));

        // decimal / bigint
        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("3.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("-3.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("-3.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("3.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.018'", "BIGINT '9'"))
                .isEqualTo(decimal("0.00200000000000000000000", createDecimalType(23, 23)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.018'", "BIGINT '9'"))
                .isEqualTo(decimal("-0.00200000000000000000000", createDecimalType(23, 23)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.018'", "BIGINT '-9'"))
                .isEqualTo(decimal("-0.00200000000000000000000", createDecimalType(23, 23)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.018'", "BIGINT '-9'"))
                .isEqualTo(decimal("0.00200000000000000000000", createDecimalType(23, 23)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '.999'", "BIGINT '9'"))
                .isEqualTo(decimal("0.11100000000000000000000", createDecimalType(23, 23)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '300'"))
                .isEqualTo(decimal("0.030000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '300'"))
                .isEqualTo(decimal("-0.030000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '-300'"))
                .isEqualTo(decimal("-0.030000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '-300'"))
                .isEqualTo(decimal("0.030000000000000000000", createDecimalType(22, 21)));
    }
}
