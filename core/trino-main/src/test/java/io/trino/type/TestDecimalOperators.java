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
import io.trino.spi.type.Int128;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDecimalOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestMapOperators.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAdd()
    {
        // short short -> short
        assertThat(assertions.operator(ADD, "DECIMAL '137.7'", "DECIMAL '17.1'"))
                .isEqualTo(decimal("0154.8", createDecimalType(5, 1)));

        assertThat(assertions.operator(ADD, "DECIMAL '-1'", "DECIMAL '-2'"))
                .isEqualTo(decimal("-03", createDecimalType(2)));

        assertThat(assertions.operator(ADD, "DECIMAL '1'", "DECIMAL '2'"))
                .isEqualTo(decimal("03", createDecimalType(2)));

        assertThat(assertions.operator(ADD, "DECIMAL '.1234567890123456'", "DECIMAL '.1234567890123456'"))
                .isEqualTo(decimal("0.2469135780246912", createDecimalType(17, 16)));

        assertThat(assertions.operator(ADD, "DECIMAL '-.1234567890123456'", "DECIMAL '-.1234567890123456'"))
                .isEqualTo(decimal("-0.2469135780246912", createDecimalType(17, 16)));

        assertThat(assertions.operator(ADD, "DECIMAL '1234567890123456'", "DECIMAL '1234567890123456'"))
                .isEqualTo(decimal("02469135780246912", createDecimalType(17)));

        // long long -> long
        assertThat(assertions.operator(ADD, "DECIMAL '123456789012345678'", "DECIMAL '123456789012345678'"))
                .isEqualTo(decimal("0246913578024691356", createDecimalType(19)));

        assertThat(assertions.operator(ADD, "DECIMAL '.123456789012345678'", "DECIMAL '.123456789012345678'"))
                .isEqualTo(decimal("0.246913578024691356", createDecimalType(19, 18)));

        assertThat(assertions.operator(ADD, "DECIMAL '1234567890123456789'", "DECIMAL '1234567890123456789'"))
                .isEqualTo(decimal("02469135780246913578", createDecimalType(20)));

        assertThat(assertions.operator(ADD, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(decimal("24691357802469135780246913578024691356", createDecimalType(38)));

        assertThat(assertions.operator(ADD, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        // negative numbers
        assertThat(assertions.operator(ADD, "DECIMAL '12345678901234567890'", "DECIMAL '-12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '-12345678901234567890'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '-12345678901234567890'", "DECIMAL '-12345678901234567890'"))
                .isEqualTo(decimal("-024691357802469135780", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '12345678901234567890'", "DECIMAL '-12345678901234567891'"))
                .isEqualTo(decimal("-000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '-12345678901234567891'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("-000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '-12345678901234567890'", "DECIMAL '12345678901234567891'"))
                .isEqualTo(decimal("000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(ADD, "DECIMAL '12345678901234567891'", "DECIMAL '-12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000001", createDecimalType(21)));

        // short short -> long
        assertThat(assertions.operator(ADD, "DECIMAL '999999999999999999'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("1999999999999999998", createDecimalType(19)));

        assertThat(assertions.operator(ADD, "DECIMAL '999999999999999999'", "DECIMAL '.999999999999999999'"))
                .isEqualTo(decimal("0999999999999999999.999999999999999999", createDecimalType(37, 18)));

        // long short -> long
        assertThat(assertions.operator(ADD, "DECIMAL '123456789012345678901234567890'", "DECIMAL '.12345678'"))
                .isEqualTo(decimal("123456789012345678901234567890.12345678", createDecimalType(38, 8)));

        assertThat(assertions.operator(ADD, "DECIMAL '.123456789012345678901234567890'", "DECIMAL '12345678'"))
                .isEqualTo(decimal("12345678.123456789012345678901234567890", createDecimalType(38, 30)));

        // short long -> long
        assertThat(assertions.operator(ADD, "DECIMAL '.12345678'", "DECIMAL '123456789012345678901234567890'"))
                .isEqualTo(decimal("123456789012345678901234567890.12345678", createDecimalType(38, 8)));

        assertThat(assertions.operator(ADD, "DECIMAL '12345678'", "DECIMAL '.123456789012345678901234567890'"))
                .isEqualTo(decimal("12345678.123456789012345678901234567890", createDecimalType(38, 30)));

        // overflow tests
        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '.1'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '1'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '-99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // max supported value for rescaling
        // this works because rescaling allows overflowed values that exceed 10^38 but still fit in 127 bits.
        // 17014000000000000000000000000000000000 * 10 is an example of such number. Both arguments and result can be stored using DECIMAL(38,0) or DECIMAL(38,1)
        assertThat(assertions.operator(ADD, "DECIMAL '17014000000000000000000000000000000000'", "DECIMAL '-7014000000000000000000000000000000000.1'"))
                .isEqualTo(decimal("9999999999999999999999999999999999999.9", createDecimalType(38, 1)));

        // 17015000000000000000000000000000000000 on the other hand is too large and rescaled to DECIMAL(38,1) it does not fit in in 127 bits
        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DECIMAL '17015000000000000000000000000000000000'", "DECIMAL '-7015000000000000000000000000000000000.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testSubtract()
    {
        // short short -> short
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '107.7'", "DECIMAL '17.1'"))
                .isEqualTo(decimal("0090.6", createDecimalType(5, 1)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-1'", "DECIMAL '-2'"))
                .isEqualTo(decimal("01", createDecimalType(2)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '1'", "DECIMAL '2'"))
                .isEqualTo(decimal("-01", createDecimalType(2)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '.1234567890123456'", "DECIMAL '.1234567890123456'"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-.1234567890123456'", "DECIMAL '-.1234567890123456'"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '1234567890123456'", "DECIMAL '1234567890123456'"))
                .isEqualTo(decimal("00000000000000000", createDecimalType(17)));

        // long long -> long
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '1234567890123456789'", "DECIMAL '1234567890123456789'"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '.1234567890123456789'", "DECIMAL '.1234567890123456789'"))
                .isEqualTo(decimal("0.0000000000000000000", createDecimalType(20, 19)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678901234567890'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-12345678901234567890'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("-024691357802469135780", createDecimalType(21)));

        // negative numbers
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678901234567890'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-12345678901234567890'", "DECIMAL '-12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-12345678901234567890'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("-024691357802469135780", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678901234567890'", "DECIMAL '12345678901234567891'"))
                .isEqualTo(decimal("-000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-12345678901234567891'", "DECIMAL '-12345678901234567890'"))
                .isEqualTo(decimal("-000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-12345678901234567890'", "DECIMAL '-12345678901234567891'"))
                .isEqualTo(decimal("000000000000000000001", createDecimalType(21)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678901234567891'", "DECIMAL '12345678901234567890'"))
                .isEqualTo(decimal("000000000000000000001", createDecimalType(21)));

        // short short -> long
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '999999999999999999'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("0000000000000000000", createDecimalType(19)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '999999999999999999'", "DECIMAL '.999999999999999999'"))
                .isEqualTo(decimal("0999999999999999998.000000000000000001", createDecimalType(37, 18)));

        // long short -> long
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '123456789012345678901234567890'", "DECIMAL '.00000001'"))
                .isEqualTo(decimal("123456789012345678901234567889.99999999", createDecimalType(38, 8)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '.000000000000000000000000000001'", "DECIMAL '87654321'"))
                .isEqualTo(decimal("-87654320.999999999999999999999999999999", createDecimalType(38, 30)));

        // short long -> long
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '.00000001'", "DECIMAL '123456789012345678901234567890'"))
                .isEqualTo(decimal("-123456789012345678901234567889.99999999", createDecimalType(38, 8)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '12345678'", "DECIMAL '.000000000000000000000000000001'"))
                .isEqualTo(decimal("12345677.999999999999999999999999999999", createDecimalType(38, 30)));

        // overflow tests
        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '.1'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '-1'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '99999999999999999999999999999999999999'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // max supported value for rescaling
        // this works because rescaling allows overflowed values that exceed 10^38 but still fit in 127 bits.
        // 17014000000000000000000000000000000000 * 10 is an example of such number. Both arguments and result can be stored using DECIMAL(38,0) or DECIMAL(38,1)
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '17014000000000000000000000000000000000'", "DECIMAL '7014000000000000000000000000000000000.1'"))
                .isEqualTo(decimal("9999999999999999999999999999999999999.9", createDecimalType(38, 1)));

        // 17015000000000000000000000000000000000 on the other hand is too large and rescaled to DECIMAL(38,1) it does not fit in in 127 bits
        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DECIMAL '17015000000000000000000000000000000000'", "DECIMAL '7015000000000000000000000000000000000.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testMultiply()
    {
        // short short -> short
        assertThat(assertions.operator(MULTIPLY, "DECIMAL '0'", "DECIMAL '1'"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '0'", "DECIMAL '-1'"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '1'", "DECIMAL '0'"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-1'", "DECIMAL '0'"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '12'", "DECIMAL '3'"))
                .isEqualTo(decimal("036", createDecimalType(3)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '12'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-036", createDecimalType(3)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-12'", "DECIMAL '3'"))
                .isEqualTo(decimal("-036", createDecimalType(3)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '1234567890123456'", "DECIMAL '3'"))
                .isEqualTo(decimal("03703703670370368", createDecimalType(17)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.1234567890123456'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.3703703670370368", createDecimalType(17, 16)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.1234567890123456'", "DECIMAL '.3'"))
                .isEqualTo(decimal(".03703703670370368", createDecimalType(17, 17)));

        // short short -> long
        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(18,0))", "CAST(1 AS DECIMAL(18,0))"))
                .isEqualTo(decimal("000000000000000000000000000000000000", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(18,0))", "CAST(-1 AS DECIMAL(18,0))"))
                .isEqualTo(decimal("000000000000000000000000000000000000", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "CAST(1 AS DECIMAL(18,0))", "CAST(0 AS DECIMAL(18,0))"))
                .isEqualTo(decimal("000000000000000000000000000000000000", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "CAST(-1 AS DECIMAL(18,0))", "CAST(0 AS DECIMAL(18,0))"))
                .isEqualTo(decimal("000000000000000000000000000000000000", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "DECIMAL '123456789012345670'"))
                .isEqualTo(decimal("01524157875323883455265967556774890", createDecimalType(35)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "DECIMAL '123456789012345670'"))
                .isEqualTo(decimal("-01524157875323883455265967556774890", createDecimalType(35)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "DECIMAL '-123456789012345670'"))
                .isEqualTo(decimal("01524157875323883455265967556774890", createDecimalType(35)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.12345678901234567'", "DECIMAL '.123456789012345670'"))
                .isEqualTo(decimal(".01524157875323883455265967556774890", createDecimalType(35, 35)));

        // long short -> long
        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(38,0))", "1"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(38,0))", "-1"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(1 AS DECIMAL(38,0))", "0"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(-1 AS DECIMAL(38,0))", "0"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '3'"))
                .isEqualTo(decimal("37037036703703703670370370367037037034", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '1234567890123456789.0123456789012345678'", "DECIMAL '3'"))
                .isEqualTo(decimal("3703703670370370367.0370370367037037034", createDecimalType(38, 19)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL '3'"))
                .isEqualTo(decimal(".37037036703703703670370370367037037034", createDecimalType(38, 38)));

        // short long -> long
        assertThat(assertions.operator(MULTIPLY, "0", "CAST(1 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "0", "CAST(-1 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "1", "CAST(0 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "-1", "CAST(0 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '3'", "DECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(decimal("37037036703703703670370370367037037034", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '3'", "DECIMAL '1234567890123456789.0123456789012345678'"))
                .isEqualTo(decimal("3703703670370370367.0370370367037037034", createDecimalType(38, 19)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '3'", "DECIMAL '.12345678901234567890123456789012345678'"))
                .isEqualTo(decimal(".37037036703703703670370370367037037034", createDecimalType(38, 38)));

        // long long -> long
        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(38,0))", "CAST(1 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(0 AS DECIMAL(38,0))", "CAST(-1 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(1 AS DECIMAL(38,0))", "CAST(0 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(-1 AS DECIMAL(38,0))", "CAST(0 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(3 AS DECIMAL(38,0))", "CAST(2 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000006", createDecimalType(38)));

        assertThat(assertions.operator(MULTIPLY, "CAST(3 AS DECIMAL(38,0))", "CAST(DECIMAL '0.2' AS DECIMAL(38,1))"))
                .isEqualTo(decimal("0000000000000000000000000000000000000.6", createDecimalType(38, 1)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.1234567890123456789'", "DECIMAL '.1234567890123456789'"))
                .isEqualTo(decimal(".01524157875323883675019051998750190521", createDecimalType(38, 38)));

        // scale exceeds max precision
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '.1234567890123456789'", "DECIMAL '.12345678901234567890'")::evaluate)
                .hasMessage("line 1:8: DECIMAL scale must be in range [0, precision (38)]: 39");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '.1'", "DECIMAL '.12345678901234567890123456789012345678'")::evaluate)
                .hasMessage("line 1:8: DECIMAL scale must be in range [0, precision (38)]: 39");

        // runtime overflow tests
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '9'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL '9'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '-9'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL '-9'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertThatThrownBy(() -> DecimalOperators.multiplyLongShortLong(Int128.valueOf("12345678901234567890123456789012345678"), 9))
                .hasMessage("Decimal overflow");

        assertThatThrownBy(() -> DecimalOperators.multiplyShortLongLong(9, Int128.valueOf("12345678901234567890123456789012345678")))
                .hasMessage("Decimal overflow");

        assertThatThrownBy(() -> DecimalOperators.multiplyLongLongLong(Int128.valueOf("12345678901234567890123456789012345678"), Int128.valueOf("9")))
                .hasMessage("Decimal overflow");
    }

    @Test
    public void testDivide()
    {
        // short short -> short
        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.3", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '0.1'"))
                .isEqualTo(decimal("10.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1.0'", "DECIMAL '9.0'"))
                .isEqualTo(decimal("00.1", createDecimalType(3, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '500.00'", "DECIMAL '0.1'"))
                .isEqualTo(decimal("5000.00", createDecimalType(6, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0333.33", createDecimalType(6, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("00333.33", createDecimalType(7, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '100.00'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal("-00333.33", createDecimalType(7, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-100.00'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("-00333.33", createDecimalType(7, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0666.67", createDecimalType(6, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00000'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("0666.66667", createDecimalType(9, 5)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200.00000'", "DECIMAL '-0.3'"))
                .isEqualTo(decimal("-0666.66667", createDecimalType(9, 5)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-200.00000'", "DECIMAL '0.3'"))
                .isEqualTo(decimal("-0666.66667", createDecimalType(9, 5)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999'", "DECIMAL '1'"))
                .isEqualTo(decimal("999999999999999999", createDecimalType(18)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '000000000000000003'"))
                .isEqualTo(decimal("3", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("03.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '999999999999999999'", "DECIMAL '500000000000000000'"))
                .isEqualTo(decimal("000000000000000002", createDecimalType(18)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        // round
        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '5'"))
                .isEqualTo(decimal("2", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '7'", "DECIMAL '5'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9'", "DECIMAL '5'"))
                .isEqualTo(decimal("-2", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-7'", "DECIMAL '5'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9'", "DECIMAL '-5'"))
                .isEqualTo(decimal("2", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-7'", "DECIMAL '-5'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9'", "DECIMAL '-5'"))
                .isEqualTo(decimal("-2", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '7'", "DECIMAL '-5'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '2'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '-2'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        // short short -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '10'", "DECIMAL '.000000001'"))
                .isEqualTo(decimal("10000000000.000000000", createDecimalType(20, 9)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-10'", "DECIMAL '.000000001'"))
                .isEqualTo(decimal("-10000000000.000000000", createDecimalType(20, 9)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '10'", "DECIMAL '-.000000001'"))
                .isEqualTo(decimal("-10000000000.000000000", createDecimalType(20, 9)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-10'", "DECIMAL '-.000000001'"))
                .isEqualTo(decimal("10000000000.000000000", createDecimalType(20, 9)));

        // long short -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '200000000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("666666666666666666666666666666666666.67", createDecimalType(38, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '200000000000000000000000000000000000'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal("-666666666666666666666666666666666666.67", createDecimalType(38, 2)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-.20000000000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal("-.66666666666666666666666666666666666667", createDecimalType(38, 38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-.20000000000000000000000000000000000000'", "DECIMAL '-0.30'"))
                .isEqualTo(decimal(".66666666666666666666666666666666666667", createDecimalType(38, 38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '.20000000000000000000000000000000000000'", "DECIMAL '0.30'"))
                .isEqualTo(decimal(".66666666666666666666666666666666666667", createDecimalType(38, 38)));

        // round
        assertThat(assertions.operator(DIVIDE, "DECIMAL '500000000000000000000000000000000075'", "DECIMAL '50'"))
                .isEqualTo(decimal("010000000000000000000000000000000002", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '500000000000000000000000000000000070'", "DECIMAL '50'"))
                .isEqualTo(decimal("010000000000000000000000000000000001", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-500000000000000000000000000000000075'", "DECIMAL '50'"))
                .isEqualTo(decimal("-010000000000000000000000000000000002", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-500000000000000000000000000000000070'", "DECIMAL '50'"))
                .isEqualTo(decimal("-010000000000000000000000000000000001", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '500000000000000000000000000000000075'", "DECIMAL '-50'"))
                .isEqualTo(decimal("-010000000000000000000000000000000002", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '500000000000000000000000000000000070'", "DECIMAL '-50'"))
                .isEqualTo(decimal("-010000000000000000000000000000000001", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-500000000000000000000000000000000075'", "DECIMAL '-50'"))
                .isEqualTo(decimal("010000000000000000000000000000000002", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-500000000000000000000000000000000070'", "DECIMAL '-50'"))
                .isEqualTo(decimal("010000000000000000000000000000000001", createDecimalType(36)));

        assertThat(assertions.operator(DIVIDE, "CAST(-1 AS DECIMAL(19,0))", "DECIMAL '2'"))
                .isEqualTo(decimal("-0000000000000000001", createDecimalType(19)));

        assertThat(assertions.operator(DIVIDE, "CAST(1 AS DECIMAL(19,0))", "DECIMAL '-2'"))
                .isEqualTo(decimal("-0000000000000000001", createDecimalType(19)));

        assertThat(assertions.operator(DIVIDE, "CAST(-1 AS DECIMAL(19,0))", "DECIMAL '3'"))
                .isEqualTo(decimal("0000000000000000000", createDecimalType(19)));

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
                .isEqualTo(decimal("03.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '1'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-1'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        // long long -> long
        assertThat(assertions.operator(DIVIDE, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '11111111111111111111111111111111111111'"))
                .isEqualTo(decimal("00000000000000000000000000000000000009", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '11111111111111111111111111111111111111'"))
                .isEqualTo(decimal("-00000000000000000000000000000000000009", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '-11111111111111111111111111111111111111'"))
                .isEqualTo(decimal("-00000000000000000000000000000000000009", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '-11111111111111111111111111111111111111'"))
                .isEqualTo(decimal("00000000000000000000000000000000000009", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '11111111111111111111111111111111111111'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-11111111111111111111111111111111111111'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '11111111111111111111111111111111111111'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-11111111111111111111111111111111111111'", "DECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '99999999999999999999999999999999999998'", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(decimal("00000000000000000000000000000000000001", createDecimalType(38)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9999999999999999999999999999999999999.8'", "DECIMAL '9999999999999999999999999999999999999.9'"))
                .isEqualTo(decimal("0000000000000000000000000000000000001.0", createDecimalType(38, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9999999999999999999999.9'", "DECIMAL '1111111111111111111111.100'"))
                .isEqualTo(decimal("0000000000000000000000009.000", createDecimalType(28, 3)));

        assertThat(assertions.operator(DIVIDE, "CAST('1635619.3155' AS DECIMAL(38,4))", "CAST('47497517.7405' AS DECIMAL(38,4))"))
                .isEqualTo(decimal("0000000000000000000000000000000000.0344", createDecimalType(38, 4)));

        // runtime overflow
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '12345678901234567890123456789012345678'", "DECIMAL '.1'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL '.1'")::evaluate)
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
                .isEqualTo(decimal("000000000000000000000000000040.00000000", createDecimalType(38, 8)));
    }

    @Test
    public void testModulus()
    {
        // short short -> short
        assertThat(assertions.operator(MODULUS, "DECIMAL '1'", "DECIMAL '3'"))
                .isEqualTo(decimal("1", createDecimalType(1, 0)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '10'", "DECIMAL '3'"))
                .isEqualTo(decimal("1", createDecimalType(1, 0)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1, 0)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0", createDecimalType(1, 0)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '10.0'", "DECIMAL '3'"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '10.0'", "DECIMAL '3.000'"))
                .isEqualTo(decimal("1.000", createDecimalType(4, 3)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "DECIMAL '3.0000000000000000'"))
                .isEqualTo(decimal("1.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.00000000000000000'", "DECIMAL '3.00000000000000000'"))
                .isEqualTo(decimal("1.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.00000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal("1.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "CAST(3 AS DECIMAL(17,0))"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '.1'", "DECIMAL '.03'"))
                .isEqualTo(decimal(".01", createDecimalType(2, 2)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '.0001'", "DECIMAL '.03'"))
                .isEqualTo(decimal(".0001", createDecimalType(4, 4)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-10'", "DECIMAL '3'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '10'", "DECIMAL '-3'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-10'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        // short long -> short
        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "CAST(3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "CAST(-3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "CAST(3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("1.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.00000000000000000'", "CAST(3 AS DECIMAL(38,17))"))
                .isEqualTo(decimal("1.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7.00000000000000000'", "CAST(3 AS DECIMAL(38,17))"))
                .isEqualTo(decimal("-1.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.0000000000000000'", "CAST(-3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("1.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7.0000000000000000'", "CAST(-3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("-1.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.00000000000000000'", "CAST(3 AS DECIMAL(38,17))"))
                .isEqualTo(decimal("0.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.00000000000000000'", "CAST(3 AS DECIMAL(38,17))"))
                .isEqualTo(decimal("0.00000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.0000000000000000'", "CAST(-3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.0000000000000000'", "CAST(-3 AS DECIMAL(38,16))"))
                .isEqualTo(decimal("0.0000000000000000", createDecimalType(17, 16)));

        // short long -> long
        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '0'", "DECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("1.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.00000000000000000'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("1.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '.01'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal(".0100000000000000000000000000000000000", createDecimalType(37, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("-1.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7'", "DECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("1.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7'", "DECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("-1.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9'", "DECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9'", "DECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9'", "DECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(decimal("0.0000000000000000000000000000000000000", createDecimalType(38, 37)));

        // long short -> short
        assertThat(assertions.operator(MODULUS, "DECIMAL '99999999999999999999999999999999999997'", "DECIMAL '3'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '99999999999999999999999999999999999997'", "DECIMAL '3.0000000000000000'"))
                .isEqualTo(decimal("1.0000000000000000", createDecimalType(17, 16)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-99999999999999999999999999999999999997'", "DECIMAL '3'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '99999999999999999999999999999999999997'", "DECIMAL '-3'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-99999999999999999999999999999999999997'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '99999999999999999999999999999999999999'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-99999999999999999999999999999999999999'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        // long short -> long
        assertThat(assertions.operator(MODULUS, "DECIMAL '0.000000000000000000000000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal(".000000000000000000000000000000000000", createDecimalType(36, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '0.000000000000000000000000000000000000'", "DECIMAL '-3'"))
                .isEqualTo(decimal(".000000000000000000000000000000000000", createDecimalType(36, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.000000000000000000000000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal("1.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7.000000000000000000000000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal("-1.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '7.000000000000000000000000000000000000'", "DECIMAL '-3'"))
                .isEqualTo(decimal("1.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-7.000000000000000000000000000000000000'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-1.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.000000000000000000000000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.000000000000000000000000000000000000'", "DECIMAL '3'"))
                .isEqualTo(decimal("0.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.000000000000000000000000000000000000'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0.000000000000000000000000000000000000", createDecimalType(37, 36)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.000000000000000000000000000000000000'", "DECIMAL '-3'"))
                .isEqualTo(decimal("0.000000000000000000000000000000000000", createDecimalType(37, 36)));

        // long long -> long
        assertThat(assertions.operator(MODULUS, "CAST(0 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(0 AS DECIMAL(38,0))", "CAST(-3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(7 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000001", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(7 AS DECIMAL(34,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("0000000000000000000000000000000001", createDecimalType(34)));

        assertThat(assertions.operator(MODULUS, "CAST(7 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(34,0))"))
                .isEqualTo(decimal("0000000000000000000000000000000001", createDecimalType(34)));

        assertThat(assertions.operator(MODULUS, "CAST(-7 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("-00000000000000000000000000000000000001", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(7 AS DECIMAL(38,0))", "CAST(-3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000001", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(-7 AS DECIMAL(38,0))", "CAST(-3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("-00000000000000000000000000000000000001", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(9 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(-9 AS DECIMAL(38,0))", "CAST(3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(9 AS DECIMAL(38,0))", "CAST(-3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.operator(MODULUS, "CAST(-9 AS DECIMAL(38,0))", "CAST(-3 AS DECIMAL(38,0))"))
                .isEqualTo(decimal("00000000000000000000000000000000000000", createDecimalType(38)));

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "DECIMAL '1'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "DECIMAL '1.000000000000000000000000000000000000'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "DECIMAL '1.000000000000000000000000000000000000'", "DECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "DECIMAL '1'", "DECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "DECIMAL '1'", "CAST(0 AS DECIMAL(38,0))")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        // short
        assertThat(assertions.operator(NEGATION, "DECIMAL '1'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '-1'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '123456.00000010' "))
                .isEqualTo(decimal("-123456.00000010", createDecimalType(14, 8)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '1234567.00500010734' "))
                .isEqualTo(decimal("-1234567.00500010734", createDecimalType(18, 11)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '-1234567.00500010734' "))
                .isEqualTo(decimal("1234567.00500010734", createDecimalType(18, 11)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '0' "))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '0.00000000000000000000' "))
                .isEqualTo(decimal(".00000000000000000000", createDecimalType(20, 20)));

        // long
        assertThat(assertions.operator(NEGATION, "DECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(decimal("-12345678901234567890123456789012345678", createDecimalType(38)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '-12345678901234567890123456789012345678'"))
                .isEqualTo(decimal("12345678901234567890123456789012345678", createDecimalType(38)));

        assertThat(assertions.operator(NEGATION, "DECIMAL '123456789012345678.90123456789012345678'"))
                .isEqualTo(decimal("-123456789012345678.90123456789012345678", createDecimalType(38, 20)));
    }

    @Test
    public void testEqual()
    {
        // short short
        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-0.000'", "DECIMAL '0.00000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '38.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0'", "DECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-37.000'", "DECIMAL '37.000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-123456789123456789'", "DECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-123456789123456789'", "DECIMAL '123456789123456789'"))
                .isEqualTo(false);

        // short long
        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '037.0'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-0.000'", "DECIMAL '0.000000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-37'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0000000000000000'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0000000000000000'", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        // long short
        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-0.000000000000000000000000000000000'", "DECIMAL '0.000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000038.0000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-00000000037.0000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000036.0000000000000000000000'", "DECIMAL '37.0000000000000000'"))
                .isEqualTo(false);

        // long long
        assertThat(assertions.operator(EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '0037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '0.0000000000000000000000000000000'", "DECIMAL '-0.000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DECIMAL '00000000038.0000000000000000000000'", "DECIMAL '000000000037.00000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DECIMAL '-00000000038.0000000000000000000000'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        // short short
        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37.0'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '0'")
                .binding("b", "DECIMAL '-0.00'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '38.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37.0'")
                .binding("b", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '-999999999999999999'")
                .binding("b", "DECIMAL '-999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '-999999999999999999'")
                .binding("b", "DECIMAL '999999999999999998'"))
                .isEqualTo(true);

        // short long
        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '0'")
                .binding("b", "DECIMAL '-0.0000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '-000000000037.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '38.0000000000000000'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '-987654321987654321'")
                .binding("b", "DECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        // long short
        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '0.0000000000000000000000000000'")
                .binding("b", "DECIMAL '-0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000038.0000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000000037.00000000000000000000'")
                .binding("b", "DECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "DECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        // long long
        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '0037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '0.00000000000000000000000000000000000'")
                .binding("b", "DECIMAL '-0.000000000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000038.0000000000000000000000'")
                .binding("b", "DECIMAL '000000000037.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "DECIMAL '00000000000037.00000000000000000000'")
                .binding("b", "DECIMAL '-00000000000037.00000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        // short short
        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '0037.0'", "DECIMAL '00036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '0037.0'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-100'", "DECIMAL '20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.operator(LESS_THAN, "DECIMAL '1'", "DECIMAL '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '1.0000000000000000'", "DECIMAL '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-123456789123456789'", "DECIMAL '-123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-123456789123456789'", "DECIMAL '123456789123456789'"))
                .isEqualTo(true);

        // short long
        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '037.0'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '037.0'", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '037.0'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-100'", "DECIMAL '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '38.0000000000000000'", "DECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-987654321987654321'", "DECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        // long short
        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000001'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-00000000000100.000000000000'", "DECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-00000000000037.00000000000000000000'", "DECIMAL '-37.0000000000000001'"))
                .isEqualTo(false);

        // long long
        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DECIMAL '-00000000000100.000000000000'", "DECIMAL '0000000020.0000000000000'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        // short short
        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '0037.0'")
                .binding("b", "DECIMAL '00038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '0037.0'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '100'")
                .binding("b", "DECIMAL '-20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '10000000000000000'")
                .binding("b", "DECIMAL '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '10000000000000000'")
                .binding("b", "DECIMAL '1.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '-123456789123456788'")
                .binding("b", "DECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '-123456789123456789'")
                .binding("b", "DECIMAL '123456789123456789'"))
                .isEqualTo(false);

        // short long
        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '36.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '100'")
                .binding("b", "DECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '38.0000000000000000'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '-987654321987654320'")
                .binding("b", "DECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        // long short
        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000001'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '0000000000100.000000000000'")
                .binding("b", "DECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000002'")
                .binding("b", "DECIMAL '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "DECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        // long long
        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '000000000036.9999999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DECIMAL '000000000000100.0000000000000000000000'")
                .binding("b", "DECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    public void testLessThanOrEqual()
    {
        // short short
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '36'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '000036.99999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '0037.0'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-100'", "DECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-123456789123456789'", "DECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-123456789123456789'", "DECIMAL '123456789123456789'"))
                .isEqualTo(true);

        // short long
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '037.0'", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '037.0'", "DECIMAL '00000000036.9999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '037.0'", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '037.0'", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-100'", "DECIMAL '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '38.0000000000000000'", "DECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-987654321987654321'", "DECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        // long short
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '000036.99999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000001'", "DECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-00000000000100.000000000000'", "DECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-00000000000037.00000000000000000000'", "DECIMAL '-37.0000000000000001'"))
                .isEqualTo(false);

        // long long
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '37.0000000000000000000000000'", "DECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '00000000037.0000000000000000000000'", "DECIMAL '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DECIMAL '-00000000000100.000000000000'", "DECIMAL '0000000020.0000000000000'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // short short
        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '000038.00001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '0037.0'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '100'")
                .binding("b", "DECIMAL '-20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '-123456789123456789'")
                .binding("b", "DECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '-123456789123456789'")
                .binding("b", "DECIMAL '123456789123456789'"))
                .isEqualTo(false);

        // short long
        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000037.0000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '36.9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37'")
                .binding("b", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '037.0'")
                .binding("b", "DECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '100'")
                .binding("b", "DECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '38.0000000000000000'")
                .binding("b", "DECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '-987654321987654321'")
                .binding("b", "DECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        // long short
        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '000037.00000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000001'")
                .binding("b", "DECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '0000000000100.000000000000'")
                .binding("b", "DECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "DECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        // long long
        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '37.0000000000000000000000000'")
                .binding("b", "DECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "DECIMAL '000000000036.9999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DECIMAL '000000000000100.0000000000000000000000'")
                .binding("b", "DECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        // short short short
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333'")
                .binding("low", "DECIMAL '-111111111111111111'")
                .binding("high", "DECIMAL '999999999999999999'"))
                .isEqualTo(true);

        // short short long
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333'")
                .binding("low", "DECIMAL '-111111111111111111'")
                .binding("high", "DECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        // short long long
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1'")
                .binding("low", "DECIMAL '-5.00000000000000000000'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6'")
                .binding("low", "DECIMAL '-5.00000000000000000000'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6'")
                .binding("low", "DECIMAL '-5.00000000000000000000'")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333'")
                .binding("low", "DECIMAL '-1111111111111111111'")
                .binding("high", "DECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        // long short short
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333.3'")
                .binding("low", "DECIMAL '-111111111111111111'")
                .binding("high", "DECIMAL '999999999999999999'"))
                .isEqualTo(true);

        // long short long
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000' "))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6.00000000000000000000'")
                .binding("low", "DECIMAL '-5'")
                .binding("high", "DECIMAL '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333.3'")
                .binding("low", "DECIMAL '-111111111111111111'")
                .binding("high", "DECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        // long long short
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '333333333333333333.3'")
                .binding("low", "DECIMAL '-111111111111111111.1'")
                .binding("high", "DECIMAL '999999999999999999'"))
                .isEqualTo(true);

        // long long long
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '1.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '-6.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DECIMAL '6.00000000000000000000'")
                .binding("low", "DECIMAL '-5.00000000000000000000' ")
                .binding("high", "DECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    public void testAddDecimalBigint()
    {
        // decimal + bigint
        assertThat(assertions.operator(ADD, "DECIMAL '123456789012345678'", "123456789012345678"))
                .isEqualTo(decimal("00246913578024691356", createDecimalType(20)));

        assertThat(assertions.operator(ADD, "DECIMAL '.123456789012345678'", "123456789012345678"))
                .isEqualTo(decimal("00123456789012345678.123456789012345678", createDecimalType(38, 18)));

        assertThat(assertions.operator(ADD, "DECIMAL '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));

        // bigint + decimal
        assertThat(assertions.operator(ADD, "123456789012345678", "DECIMAL '123456789012345678'"))
                .isEqualTo(decimal("00246913578024691356", createDecimalType(20)));

        assertThat(assertions.operator(ADD, "123456789012345678", "DECIMAL '.123456789012345678'"))
                .isEqualTo(decimal("00123456789012345678.123456789012345678", createDecimalType(38, 18)));

        assertThat(assertions.operator(ADD, "1234567890123456789", "DECIMAL '-1234567890123456789'"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));
    }

    @Test
    public void testSubtractDecimalBigint()
    {
        // decimal - bigint
        assertThat(assertions.operator(SUBTRACT, "DECIMAL '1234567890123456789'", "1234567890123456789"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '.1234567890123456789'", "1234567890123456789"))
                .isEqualTo(decimal("-1234567890123456788.8765432109876543211", createDecimalType(38, 19)));

        assertThat(assertions.operator(SUBTRACT, "DECIMAL '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(decimal("-02469135780246913578", createDecimalType(20)));

        // bigint - decimal
        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "DECIMAL '1234567890123456789'"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));

        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "DECIMAL '.1234567890123456789'"))
                .isEqualTo(decimal("1234567890123456788.8765432109876543211", createDecimalType(38, 19)));

        assertThat(assertions.operator(SUBTRACT, "-1234567890123456789", "DECIMAL '1234567890123456789'"))
                .isEqualTo(decimal("-02469135780246913578", createDecimalType(20)));
    }

    @Test
    public void testMultiplyDecimalBigint()
    {
        // decimal bigint
        assertThat(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "12345678901234567"))
                .isEqualTo(decimal("000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "12345678901234567"))
                .isEqualTo(decimal("-000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "-12345678901234567"))
                .isEqualTo(decimal("000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.1234567890'", "BIGINT '3'"))
                .isEqualTo(decimal("0000000000000000000.3703703670", createDecimalType(29, 10)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '.1234567890'", "BIGINT '0'"))
                .isEqualTo(decimal("0000000000000000000.0000000000", createDecimalType(29, 10)));

        assertThat(assertions.operator(MULTIPLY, "DECIMAL '-.1234567890'", "BIGINT '0'"))
                .isEqualTo(decimal("0000000000000000000.0000000000", createDecimalType(29, 10)));

        // bigint decimal
        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "DECIMAL '12345678901234567'"))
                .isEqualTo(decimal("000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "DECIMAL '-12345678901234567'"))
                .isEqualTo(decimal("-000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "-12345678901234567", "DECIMAL '-12345678901234567'"))
                .isEqualTo(decimal("000152415787532388345526596755677489", createDecimalType(36)));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "DECIMAL '.1234567890'"))
                .isEqualTo(decimal("0000000000000000000.3703703670", createDecimalType(29, 10)));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "DECIMAL '.0000000000'"))
                .isEqualTo(decimal("0000000000000000000.0000000000", createDecimalType(29, 10)));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '-3'", "DECIMAL '.0000000000'"))
                .isEqualTo(decimal("0000000000000000000.0000000000", createDecimalType(29, 10)));
    }

    @Test
    public void testDivideDecimalBigint()
    {
        // bigint / decimal
        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("00000000000000000003.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '3.0'"))
                .isEqualTo(decimal("-00000000000000000003.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '-3.0'"))
                .isEqualTo(decimal("-00000000000000000003.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '-3.0'"))
                .isEqualTo(decimal("00000000000000000003.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '000000000000000003.0'"))
                .isEqualTo(decimal("00000000000000000003.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '18'", "DECIMAL '0.01'"))
                .isEqualTo(decimal("000000000000000001800.00", createDecimalType(23, 2)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '00000000000000000.1'"))
                .isEqualTo(decimal("00000000000000000090.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '300.0'"))
                .isEqualTo(decimal("00000000000000000000.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '300.0'"))
                .isEqualTo(decimal("00000000000000000000.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "DECIMAL '-300.0'"))
                .isEqualTo(decimal("00000000000000000000.0", createDecimalType(21, 1)));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "DECIMAL '-300.0'"))
                .isEqualTo(decimal("00000000000000000000.0", createDecimalType(21, 1)));

        // decimal / bigint
        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("3.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("-3.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("-3.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("3.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.018'", "BIGINT '9'"))
                .isEqualTo(decimal(".002", createDecimalType(3, 3)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.018'", "BIGINT '9'"))
                .isEqualTo(decimal("-.002", createDecimalType(3, 3)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '0.018'", "BIGINT '-9'"))
                .isEqualTo(decimal("-.002", createDecimalType(3, 3)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-0.018'", "BIGINT '-9'"))
                .isEqualTo(decimal(".002", createDecimalType(3, 3)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '.999'", "BIGINT '9'"))
                .isEqualTo(decimal(".111", createDecimalType(3, 3)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '300'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '300'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '9.0'", "BIGINT '-300'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(DIVIDE, "DECIMAL '-9.0'", "BIGINT '-300'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));
    }

    @Test
    public void testModulusDecimalBigint()
    {
        // bigint % decimal
        assertThat(assertions.operator(MODULUS, "BIGINT '13'", "DECIMAL '9.0'"))
                .isEqualTo(decimal("4.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "BIGINT '18'", "DECIMAL '0.01'"))
                .isEqualTo(decimal(".00", createDecimalType(2, 2)));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "DECIMAL '.1'"))
                .isEqualTo(decimal(".0", createDecimalType(1, 1)));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "DECIMAL '.1'"))
                .isEqualTo(decimal(".0", createDecimalType(1, 1)));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "DECIMAL '-.1'"))
                .isEqualTo(decimal(".0", createDecimalType(1, 1)));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "DECIMAL '-.1'"))
                .isEqualTo(decimal(".0", createDecimalType(1, 1)));

        // decimal % bigint
        assertThat(assertions.operator(MODULUS, "DECIMAL '13.0'", "BIGINT '9'"))
                .isEqualTo(decimal("04.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-13.0'", "BIGINT '9'"))
                .isEqualTo(decimal("-04.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '13.0'", "BIGINT '-9'"))
                .isEqualTo(decimal("04.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-13.0'", "BIGINT '-9'"))
                .isEqualTo(decimal("-04.0", createDecimalType(3, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '18.00'", "BIGINT '3'"))
                .isEqualTo(decimal("00.00", createDecimalType(4, 2)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.0'", "BIGINT '3'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '-9.0'", "BIGINT '-3'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.operator(MODULUS, "DECIMAL '5.128'", "BIGINT '2'"))
                .isEqualTo(decimal("1.128", createDecimalType(4, 3)));
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL)", "CAST(NULL AS DECIMAL)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '37'", "DECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "38"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "37"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "NULL"))
                .isEqualTo(true);

        // short short
        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '-2'", "DECIMAL '-3'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '-2'", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(2,0))", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '-2'", "DECIMAL '-2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(1,0))", "DECIMAL '-2'"))
                .isEqualTo(true);

        // long long
        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '12345678901234567.8902345678901234567'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS DECIMAL(36,1))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(36,1))", "CAST(NULL AS DECIMAL(27,3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '-12345678901234567.89012345678901234567'", "DECIMAL '-12345678901234567.89012345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(36,1))", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(true);

        // short long
        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '-3'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(36,1))", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '00000000000000007.80000000000000000000'", "DECIMAL '7.8'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DECIMAL(36,1))", "DECIMAL '7.8'"))
                .isEqualTo(true);

        // with unknown
        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "DECIMAL '-2'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '-2'", "NULL"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DECIMAL '12345678901234567.89012345678901234567'", "NULL"))
                .isEqualTo(true);

        // delegation from other operator (exercises block-position convention implementation)
        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1.23, 4.56]", "ARRAY [1.23, 4.56]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1.23, NULL]", "ARRAY [1.23, 4.56]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1.23, NULL]", "ARRAY [NULL, 4.56]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1234567890.123456789, 9876543210.987654321]", "ARRAY [1234567890.123456789, 9876543210.987654321]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1234567890.123456789, NULL]", "ARRAY [1234567890.123456789, 9876543210.987654321]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY [1234567890.123456789, NULL]", "ARRAY [NULL, 9876543210.987654321]"))
                .isEqualTo(true);
    }

    @Test
    public void testNullIf()
    {
        // short short
        assertThat(assertions.function("nullif", "DECIMAL '-2'", "DECIMAL '-3'"))
                .isEqualTo(decimal("-2", createDecimalType(1, 0)));

        assertThat(assertions.function("nullif", "DECIMAL '-2'", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(decimal("-2", createDecimalType(1, 0)));

        assertThat(assertions.function("nullif", "DECIMAL '-2'", "DECIMAL '-2'"))
                .isNull(createDecimalType(1, 0));

        assertThat(assertions.function("nullif", "CAST(NULL AS DECIMAL(1, 0))", "DECIMAL '-2'"))
                .isNull(createDecimalType(1, 0));

        assertThat(assertions.function("nullif", "CAST(NULL AS DECIMAL(1, 0))", "CAST(NULL AS DECIMAL(1,0))"))
                .isNull(createDecimalType(1, 0));

        // long long
        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '12345678901234567.8902345678901234567'"))
                .isEqualTo(decimal("12345678901234567.89012345678901234567", createDecimalType(37, 20)));

        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS DECIMAL(36,1))"))
                .isEqualTo(decimal("12345678901234567.89012345678901234567", createDecimalType(37, 20)));

        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(createDecimalType(37, 20));

        assertThat(assertions.function("nullif", "CAST(NULL AS DECIMAL(38, 0))", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(createDecimalType(38, 0));

        assertThat(assertions.function("nullif", "CAST(NULL AS DECIMAL(38, 0))", "CAST(NULL AS DECIMAL(38,0))"))
                .isNull(createDecimalType(38, 0));

        // short long
        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '-3'"))
                .isEqualTo(decimal("12345678901234567.89012345678901234567", createDecimalType(37, 20)));

        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS DECIMAL(1,0))"))
                .isEqualTo(decimal("12345678901234567.89012345678901234567", createDecimalType(37, 20)));

        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(createDecimalType(37, 20));

        assertThat(assertions.function("nullif", "CAST(NULL AS DECIMAL(1, 0))", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(createDecimalType(1, 0));

        // with unknown
        assertThat(assertions.function("nullif", "NULL", "NULL"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "NULL", "DECIMAL '-2'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "DECIMAL '-2'", "NULL"))
                .isEqualTo(decimal("-2", createDecimalType(1, 0)));

        assertThat(assertions.function("nullif", "NULL", "DECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "DECIMAL '12345678901234567.89012345678901234567'", "NULL"))
                .isEqualTo(decimal("12345678901234567.89012345678901234567", createDecimalType(37, 20)));
    }

    @Test
    public void testCoalesce()
    {
        assertThat(assertions.function("coalesce", "2.1", "null", "cast(null as decimal(5,3))"))
                .isEqualTo(decimal("02.100", createDecimalType(5, 3)));

        assertThat(assertions.function("coalesce", "cast(null as decimal(17, 3))", "null", "cast(null as decimal(12,3))"))
                .isNull(createDecimalType(17, 3));

        assertThat(assertions.function("coalesce", "3", "2.1", "null", "cast(null as decimal(6,3))"))
                .isEqualTo(decimal("0000000003.000", createDecimalType(13, 3)));

        assertThat(assertions.function("coalesce", "cast(null as decimal(17, 3))", "null", "cast(null as decimal(12,3))"))
                .isNull(createDecimalType(17, 3));
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as DECIMAL)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "cast(null as DECIMAL(37,3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "DECIMAL '.999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "DECIMAL '18'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "DECIMAL '9.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "DECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);
    }
}
