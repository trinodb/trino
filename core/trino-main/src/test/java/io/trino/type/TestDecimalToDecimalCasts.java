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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDecimalToDecimalCasts
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testShortDecimalToShortDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(1, 0))")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 2))")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(decimal("0.00", createDecimalType(3, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(1, 0))")
                .binding("a", "DECIMAL '2'"))
                .isEqualTo(decimal("2", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(1, 0))")
                .binding("a", "DECIMAL '-2'"))
                .isEqualTo(decimal("-2", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '2.0'"))
                .isEqualTo(decimal("2.0", createDecimalType(2, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '-2.0'"))
                .isEqualTo(decimal("-2.0", createDecimalType(2, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "DECIMAL '2.0'"))
                .isEqualTo(decimal("02", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "DECIMAL '-2.0'"))
                .isEqualTo(decimal("-02", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 2))")
                .binding("a", "DECIMAL '2.0'"))
                .isEqualTo(decimal("2.00", createDecimalType(3, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 2))")
                .binding("a", "DECIMAL '-2.0'"))
                .isEqualTo(decimal("-2.00", createDecimalType(3, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '1.449'"))
                .isEqualTo(decimal("1.4", createDecimalType(2, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '1.459'"))
                .isEqualTo(decimal("1.5", createDecimalType(2, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '-1.449'"))
                .isEqualTo(decimal("-1.4", createDecimalType(2, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 1))")
                .binding("a", "DECIMAL '-1.459'"))
                .isEqualTo(decimal("-1.5", createDecimalType(2, 1)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "DECIMAL '12345.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(6, 1) '12345.6' to DECIMAL(4, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "DECIMAL '-12345.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(6, 1) '-12345.6' to DECIMAL(4, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "DECIMAL '12345.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(6, 1) '12345.6' to DECIMAL(4, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "DECIMAL '-12345.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(6, 1) '-12345.6' to DECIMAL(4, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testShortDecimalToLongDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '1.2345'"))
                .isEqualTo(decimal("1.23450000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '-1.2345'"))
                .isEqualTo(decimal("-1.23450000000000000000", createDecimalType(21, 20)));
    }

    @Test
    public void testLongDecimalToShortDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(5, 4))")
                .binding("a", "DECIMAL '1.23450000000000000000'"))
                .isEqualTo(decimal("1.2345", createDecimalType(5, 4)));

        assertThat(assertions.expression("cast(a as DECIMAL(5, 4))")
                .binding("a", "DECIMAL '-1.23450000000000000000'"))
                .isEqualTo(decimal("-1.2345", createDecimalType(5, 4)));
    }

    @Test
    public void testLongDecimalToLongDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo(decimal("0.00000000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(22, 20))")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo(decimal("00.00000000000000000000", createDecimalType(22, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(23, 20))")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo(decimal("000.00000000000000000000", createDecimalType(23, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 19))")
                .binding("a", "DECIMAL '2.00000000000000000000'"))
                .isEqualTo(decimal("2.0000000000000000000", createDecimalType(20, 19)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 19))")
                .binding("a", "DECIMAL '-2.00000000000000000000'"))
                .isEqualTo(decimal("-2.0000000000000000000", createDecimalType(20, 19)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '2.00000000000000000000'"))
                .isEqualTo(decimal("2.00000000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '-2.00000000000000000000'"))
                .isEqualTo(decimal("-2.00000000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(22, 20))")
                .binding("a", "DECIMAL '2.00000000000000000000'"))
                .isEqualTo(decimal("02.00000000000000000000", createDecimalType(22, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(22, 20))")
                .binding("a", "DECIMAL '-2.00000000000000000000'"))
                .isEqualTo(decimal("-02.00000000000000000000", createDecimalType(22, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(22, 21))")
                .binding("a", "DECIMAL '2.00000000000000000000'"))
                .isEqualTo(decimal("2.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.expression("cast(a as DECIMAL(22, 21))")
                .binding("a", "DECIMAL '-2.00000000000000000000'"))
                .isEqualTo(decimal("-2.000000000000000000000", createDecimalType(22, 21)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '1.000000000000000000004'"))
                .isEqualTo(decimal("1.00000000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '1.000000000000000000005'"))
                .isEqualTo(decimal("1.00000000000000000001", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '-1.000000000000000000004'"))
                .isEqualTo(decimal("-1.00000000000000000000", createDecimalType(21, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(21, 20))")
                .binding("a", "DECIMAL '-1.000000000000000000005'"))
                .isEqualTo(decimal("-1.00000000000000000001", createDecimalType(21, 20)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DECIMAL '1234500000000000000000000.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(26, 1) '1234500000000000000000000.6' to DECIMAL(20, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DECIMAL '-1234500000000000000000000.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(26, 1) '-1234500000000000000000000.6' to DECIMAL(20, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(22,2))")
                .binding("a", "DECIMAL '1234500000000000000000000.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(26, 1) '1234500000000000000000000.6' to DECIMAL(22, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(22,2))")
                .binding("a", "DECIMAL '-1234500000000000000000000.6'").evaluate())
                .hasMessage("Cannot cast DECIMAL(26, 1) '-1234500000000000000000000.6' to DECIMAL(22, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }
}
