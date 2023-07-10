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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDecimalCasts
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
    public void testBooleanToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "true"))
                .isEqualTo(decimal("01", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 1))")
                .binding("a", "true"))
                .isEqualTo(decimal("01.0", createDecimalType(3, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL)")
                .binding("a", "true"))
                .isEqualTo(decimal("1", createDecimalType(38)));

        assertThat(assertions.expression("cast(a as DECIMAL(2))")
                .binding("a", "true"))
                .isEqualTo(decimal("01", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "false"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(2))")
                .binding("a", "false"))
                .isEqualTo(decimal("00", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL)")
                .binding("a", "false"))
                .isEqualTo(decimal("0", createDecimalType(38)));

        assertThat(assertions.expression("cast(a as DECIMAL(18, 0))")
                .binding("a", "true"))
                .isEqualTo(decimal("000000000000000001", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(18, 2))")
                .binding("a", "false"))
                .isEqualTo(decimal("0000000000000000.00", createDecimalType(18, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "true"))
                .isEqualTo(decimal("0000000001.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "false"))
                .isEqualTo(decimal("0000000000.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "true"))
                .isEqualTo(decimal("0000000001.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "false"))
                .isEqualTo(decimal("0000000000.00000000000000000000", createDecimalType(30, 20)));
    }

    @Test
    public void testDecimalToBooleanCasts()
    {
        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '1.1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '-1.1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '0.0000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '00000000000000001.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '000000000000000.000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '0000000001.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "DECIMAL '0000000000.00000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    public void testBigintToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "BIGINT '234'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "BIGINT '234'"))
                .isEqualTo(decimal("234.00", createDecimalType(5, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "BIGINT '234'"))
                .isEqualTo(decimal("0234", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "BIGINT '-234'"))
                .isEqualTo(decimal("-234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "BIGINT '0'"))
                .isEqualTo(decimal("00.00", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(17, 0))")
                .binding("a", "BIGINT '12345678901234567'"))
                .isEqualTo(decimal("12345678901234567", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(18, 0))")
                .binding("a", "BIGINT '123456789012345679'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "BIGINT '1234567890'"))
                .isEqualTo(decimal("1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "BIGINT '-1234567890'"))
                .isEqualTo(decimal("-1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "BIGINT '1234567890'"))
                .isEqualTo(decimal("1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "BIGINT '-1234567890'"))
                .isEqualTo(decimal("-1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(17, 10))")
                .binding("a", "BIGINT '-1234567'"))
                .isEqualTo(decimal("-1234567.0000000000", createDecimalType(17, 10)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "BIGINT '10'").evaluate())
                .hasMessage("Cannot cast BIGINT '10' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,10))")
                .binding("a", "BIGINT '1234567890'").evaluate())
                .hasMessage("Cannot cast BIGINT '1234567890' to DECIMAL(17, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "BIGINT '123'").evaluate())
                .hasMessage("Cannot cast BIGINT '123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "BIGINT '-123'").evaluate())
                .hasMessage("Cannot cast BIGINT '-123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,1))")
                .binding("a", "BIGINT '123456789012345678'").evaluate())
                .hasMessage("Cannot cast BIGINT '123456789012345678' to DECIMAL(17, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "BIGINT '12345678901'").evaluate())
                .hasMessage("Cannot cast BIGINT '12345678901' to DECIMAL(20, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testIntegerToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "INTEGER '234'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "INTEGER '234'"))
                .isEqualTo(decimal("234.00", createDecimalType(5, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "INTEGER '234'"))
                .isEqualTo(decimal("0234", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "INTEGER '-234'"))
                .isEqualTo(decimal("-234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "INTEGER '0'"))
                .isEqualTo(decimal("00.00", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,8))")
                .binding("a", "INTEGER '1345678901'"))
                .isEqualTo(decimal("1345678901.00000000", createDecimalType(18, 8)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "INTEGER '1234567890'"))
                .isEqualTo(decimal("1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "INTEGER '-1234567890'"))
                .isEqualTo(decimal("-1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "INTEGER '1234567890'"))
                .isEqualTo(decimal("1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "INTEGER '-1234567890'"))
                .isEqualTo(decimal("-1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "INTEGER '10'").evaluate())
                .hasMessage("Cannot cast INTEGER '10' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,10))")
                .binding("a", "INTEGER '1234567890'").evaluate())
                .hasMessage("Cannot cast INTEGER '1234567890' to DECIMAL(17, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "INTEGER '123'").evaluate())
                .hasMessage("Cannot cast INTEGER '123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "INTEGER '-123'").evaluate())
                .hasMessage("Cannot cast INTEGER '-123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testSmallintToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "SMALLINT '234'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "SMALLINT '234'"))
                .isEqualTo(decimal("234.00", createDecimalType(5, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "SMALLINT '234'"))
                .isEqualTo(decimal("0234", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "SMALLINT '-234'"))
                .isEqualTo(decimal("-234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "SMALLINT '0'"))
                .isEqualTo(decimal("00.00", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "SMALLINT '1234'"))
                .isEqualTo(decimal("0000001234.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "SMALLINT '-1234'"))
                .isEqualTo(decimal("-0000001234.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "SMALLINT '1234'"))
                .isEqualTo(decimal("0000001234.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "SMALLINT '-1234'"))
                .isEqualTo(decimal("-0000001234.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,13))")
                .binding("a", "SMALLINT '12345'"))
                .isEqualTo(decimal("12345.0000000000000", createDecimalType(18, 13)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "SMALLINT '10'").evaluate())
                .hasMessage("Cannot cast SMALLINT '10' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,14))")
                .binding("a", "SMALLINT '1234'").evaluate())
                .hasMessage("Cannot cast SMALLINT '1234' to DECIMAL(17, 14)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "SMALLINT '123'").evaluate())
                .hasMessage("Cannot cast SMALLINT '123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "SMALLINT '-123'").evaluate())
                .hasMessage("Cannot cast SMALLINT '-123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testTinyintToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "TINYINT '23'"))
                .isEqualTo(decimal("023.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "TINYINT '23'"))
                .isEqualTo(decimal("023.00", createDecimalType(5, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "TINYINT '23'"))
                .isEqualTo(decimal("0023", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "TINYINT '-23'"))
                .isEqualTo(decimal("-023.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,2))")
                .binding("a", "TINYINT '0'"))
                .isEqualTo(decimal("00.00", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "TINYINT '123'"))
                .isEqualTo(decimal("0000000123.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "TINYINT '-123'"))
                .isEqualTo(decimal("-0000000123.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "TINYINT '123'"))
                .isEqualTo(decimal("0000000123.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30, 20))")
                .binding("a", "TINYINT '-123'"))
                .isEqualTo(decimal("-0000000123.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,15))")
                .binding("a", "TINYINT '123'"))
                .isEqualTo(decimal("123.000000000000000", createDecimalType(18, 15)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "TINYINT '10'").evaluate())
                .hasMessage("Cannot cast TINYINT '10' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,15))")
                .binding("a", "TINYINT '123'").evaluate())
                .hasMessage("Cannot cast TINYINT '123' to DECIMAL(17, 15)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "TINYINT '123'").evaluate())
                .hasMessage("Cannot cast TINYINT '123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,1))")
                .binding("a", "TINYINT '-123'").evaluate())
                .hasMessage("Cannot cast TINYINT '-123' to DECIMAL(2, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToBigintCasts()
    {
        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo(2L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '2.5'"))
                .isEqualTo(3L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '2.49'"))
                .isEqualTo(2L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '20'"))
                .isEqualTo(20L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo(1L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '-20'"))
                .isEqualTo(-20L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '-1'"))
                .isEqualTo(-1L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo(-2L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '-2.5'"))
                .isEqualTo(-3L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '0.1234567890123456'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '0.9999999999999999'"))
                .isEqualTo(1L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '0.99999999999999999999'"))
                .isEqualTo(1L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '123.999999999999999'"))
                .isEqualTo(124L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '999999999999999999'"))
                .isEqualTo(999999999999999999L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .isEqualTo(1234567890L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .isEqualTo(-1234567890L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "DECIMAL '12345678901234567890'").evaluate())
                .hasMessage("Cannot cast '12345678901234567890' to BIGINT")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToIntegerCasts()
    {
        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo(2);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '2.5'"))
                .isEqualTo(3);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '2.49'"))
                .isEqualTo(2);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '20'"))
                .isEqualTo(20);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo(1);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(0);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '-20'"))
                .isEqualTo(-20);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '-1'"))
                .isEqualTo(-1);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo(-2);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '-2.5'"))
                .isEqualTo(-3);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '0.1234567890123456'"))
                .isEqualTo(0);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '0.9999999999999999'"))
                .isEqualTo(1);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo(0);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '0.99999999999999999999'"))
                .isEqualTo(1);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '123.999999999999999'"))
                .isEqualTo(124);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .isEqualTo(1234567890);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .isEqualTo(-1234567890);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "DECIMAL '12345678901234567890'").evaluate())
                .hasMessage("Cannot cast '12345678901234567890' to INTEGER")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToSmallintCasts()
    {
        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo((short) 2);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '2.5'"))
                .isEqualTo((short) 3);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '2.49'"))
                .isEqualTo((short) 2);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '20'"))
                .isEqualTo((short) 20);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo((short) 1);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo((short) 0);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '-20'"))
                .isEqualTo((short) -20);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '-1'"))
                .isEqualTo((short) -1);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo((short) -2);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '-2.5'"))
                .isEqualTo((short) -3);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '0.1234567890123456'"))
                .isEqualTo((short) 0);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '0.9999999999999999'"))
                .isEqualTo((short) 1);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo((short) 0);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '0.99999999999999999999'"))
                .isEqualTo((short) 1);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '123.999999999999999'"))
                .isEqualTo((short) 124);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '1234.1234567890'"))
                .isEqualTo((short) 1234);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '-1234.1234567890'"))
                .isEqualTo((short) -1234);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "DECIMAL '12345678901234567890'").evaluate())
                .hasMessage("Cannot cast '12345678901234567890' to SMALLINT")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToTinyintCasts()
    {
        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo((byte) 2);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '2.5'"))
                .isEqualTo((byte) 3);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '2.49'"))
                .isEqualTo((byte) 2);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '20'"))
                .isEqualTo((byte) 20);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '-20'"))
                .isEqualTo((byte) -20);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '-1'"))
                .isEqualTo((byte) -1);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo((byte) -2);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '-2.5'"))
                .isEqualTo((byte) -3);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '0.1234567890123456'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '0.9999999999999999'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '0.00000000000000000000'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '0.99999999999999999999'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '123.999999999999999'"))
                .isEqualTo((byte) 124);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '12.1234567890'"))
                .isEqualTo((byte) 12);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '-12.1234567890'"))
                .isEqualTo((byte) -12);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "DECIMAL '12345678901234567890'").evaluate())
                .hasMessage("Cannot cast '12345678901234567890' to TINYINT")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDoubleToShortDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "DOUBLE '234.0'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "DOUBLE '.01'"))
                .isEqualTo(decimal(".010", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "DOUBLE '.0'"))
                .isEqualTo(decimal(".000", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(1,0))")
                .binding("a", "DOUBLE '0.0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "DOUBLE '0.0'"))
                .isEqualTo(decimal("0000", createDecimalType(4, 0)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "DOUBLE '1000.0'"))
                .isEqualTo(decimal("1000", createDecimalType(4, 0)));

        assertThat(assertions.expression("cast(a as DECIMAL(7,2))")
                .binding("a", "DOUBLE '1000.01'"))
                .isEqualTo(decimal("01000.01", createDecimalType(7, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,0))")
                .binding("a", "DOUBLE '-234.0'"))
                .isEqualTo(decimal("-234", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '1234567890123456.0'"))
                .isEqualTo(decimal("1234567890123456", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '-1234567890123456.0'"))
                .isEqualTo(decimal("-1234567890123456", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "DOUBLE '1234567890123456.0'"))
                .isEqualTo(decimal("01234567890123456", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "DOUBLE '-1234567890123456.0'"))
                .isEqualTo(decimal("-01234567890123456", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "DOUBLE '1234567890.0'"))
                .isEqualTo(decimal("1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "DOUBLE '-1234567890.0'"))
                .isEqualTo(decimal("-1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "DOUBLE '1234567890.0'"))
                .isEqualTo(decimal("1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "DOUBLE '-1234567890.0'"))
                .isEqualTo(decimal("-1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,0))")
                .binding("a", "DOUBLE '123456789123456784'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("123456789123456780", createDecimalType(18))
                        : decimal("123456789123456784", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,9))")
                .binding("a", "DOUBLE '123456789.123456790'"))
                .isEqualTo(decimal("123456789.123456790", createDecimalType(18, 9)));

        // test rounding
        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '1234567890.49'"))
                .isEqualTo(decimal("0000001234567890", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '1234567890.51'"))
                .isEqualTo(decimal("0000001234567891", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '-1234567890.49'"))
                .isEqualTo(decimal("-0000001234567890", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '-1234567890.51'"))
                .isEqualTo(decimal("-0000001234567891", createDecimalType(16)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,16))")
                .binding("a", "DOUBLE '100.02'").evaluate())
                .hasMessage("Cannot cast DOUBLE '100.02' to DECIMAL(17, 16)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "DOUBLE '234.0'").evaluate())
                .hasMessage("Cannot cast DOUBLE '234.0' to DECIMAL(2, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "DOUBLE '1000.01'").evaluate())
                .hasMessage("Cannot cast DOUBLE '1000.01' to DECIMAL(5, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "DOUBLE '-234.0'").evaluate())
                .hasMessage("Cannot cast DOUBLE '-234.0' to DECIMAL(2, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,16))")
                .binding("a", "infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'Infinity' to DECIMAL(17, 16)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,5))")
                .binding("a", "nan()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'NaN' to DECIMAL(10, 5)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,1))")
                .binding("a", "infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'Infinity' to DECIMAL(10, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(1,1))")
                .binding("a", "-infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE '-Infinity' to DECIMAL(1, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDoubleToLongDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(20,1))")
                .binding("a", "DOUBLE '234.0'"))
                .isEqualTo(decimal("0000000000000000234.0", createDecimalType(20, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,5))")
                .binding("a", "DOUBLE '.25'"))
                .isEqualTo(decimal("000000000000000.25000", createDecimalType(20, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,3))")
                .binding("a", "DOUBLE '.01'"))
                .isEqualTo(decimal("00000000000000000.010", createDecimalType(20, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,3))")
                .binding("a", "DOUBLE '.0'"))
                .isEqualTo(decimal("00000000000000000.000", createDecimalType(20, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '0.0'"))
                .isEqualTo(decimal("00000000000000000000", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,2))")
                .binding("a", "DOUBLE '1000.01'"))
                .isEqualTo(decimal("000000000000001000.01", createDecimalType(20, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '-234.0'"))
                .isEqualTo(decimal("-00000000000000000234", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '12345678901234567.0'"))
                .isEqualTo(decimal("00012345678901234568", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '-12345678901234567.0'"))
                .isEqualTo(decimal("-00012345678901234568", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "DOUBLE '1234567890.0'"))
                .isEqualTo(decimal("1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "DOUBLE '-1234567890.0'"))
                .isEqualTo(decimal("-1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '1234567890123456.9'"))
                .isEqualTo(decimal("1234567890123457", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(16,0))")
                .binding("a", "DOUBLE '-1234567890123456.9'"))
                .isEqualTo(decimal("-1234567890123457", createDecimalType(16)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,5))")
                .binding("a", "DOUBLE '1234567890123456789012345'"))
                .isEqualTo(decimal("1234567890123456800000000.00000", createDecimalType(30, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,5))")
                .binding("a", "DOUBLE '-1234567890123456789012345'"))
                .isEqualTo(decimal("-1234567890123456800000000.00000", createDecimalType(30, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,5))")
                .binding("a", "DOUBLE '1.2345678901234568E24'"))
                .isEqualTo(decimal("1234567890123456800000000.00000", createDecimalType(30, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,5))")
                .binding("a", "DOUBLE '-1.2345678901234568E24'"))
                .isEqualTo(decimal("-1234567890123456800000000.00000", createDecimalType(30, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,30))")
                .binding("a", "DOUBLE '.1234567890123456789012345'"))
                .isEqualTo(decimal(".123456789012345680000000000000", createDecimalType(30, 30)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,30))")
                .binding("a", "DOUBLE '-.1234567890123456789012345'"))
                .isEqualTo(decimal("-.123456789012345680000000000000", createDecimalType(30, 30)));

        // test roundtrip
        assertThat(assertions.expression("CAST(CAST(DOUBLE '1234567890123456789012345' AS DECIMAL(30,5)) as DOUBLE) = DOUBLE '1234567890123456789012345'"))
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(CAST(DOUBLE '1.2345678901234568E24' AS DECIMAL(30,5)) as DOUBLE) = DOUBLE '1.2345678901234568E24'"))
                .isEqualTo(true);

        // test rounding
        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '1234567890.49'"))
                .isEqualTo(decimal("00000000001234567890", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '1234567890.51'"))
                .isEqualTo(decimal("00000000001234567891", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '-1234567890.49'"))
                .isEqualTo(decimal("-00000000001234567890", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '-1234567890.51'"))
                .isEqualTo(decimal("-00000000001234567891", createDecimalType(20)));

        assertThat(assertions.expression("cast(a as DECIMAL(10,0))")
                .binding("a", "DOUBLE '1234567890.49'"))
                .isEqualTo(decimal("1234567890", createDecimalType(10)));

        assertThat(assertions.expression("cast(a as DECIMAL(10,0))")
                .binding("a", "DOUBLE '1234567890.51'"))
                .isEqualTo(decimal("1234567891", createDecimalType(10)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "DOUBLE '100.02'").evaluate())
                .hasMessage("Cannot cast DOUBLE '100.02' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '234000000000000000000.0'").evaluate())
                .hasMessage("Cannot cast DOUBLE '2.34E20' to DECIMAL(20, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,2))")
                .binding("a", "DOUBLE '1000000000000000000.01'").evaluate())
                .hasMessage("Cannot cast DOUBLE '1.0E18' to DECIMAL(20, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,0))")
                .binding("a", "DOUBLE '-234000000000000000000.0'").evaluate())
                .hasMessage("Cannot cast DOUBLE '-2.34E20' to DECIMAL(20, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "DOUBLE '12345678901.1'").evaluate())
                .hasMessage("Cannot cast DOUBLE '1.23456789011E10' to DECIMAL(20, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'Infinity' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,10))")
                .binding("a", "nan()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'NaN' to DECIMAL(38, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,2))")
                .binding("a", "infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'Infinity' to DECIMAL(38, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,1))")
                .binding("a", "-infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE '-Infinity' to DECIMAL(38, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,5))")
                .binding("a", "nan()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'NaN' to DECIMAL(10, 5)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,1))")
                .binding("a", "infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE 'Infinity' to DECIMAL(10, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(1,1))")
                .binding("a", "-infinity()").evaluate())
                .hasMessage("Cannot cast DOUBLE '-Infinity' to DECIMAL(1, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToDoubleCasts()
    {
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo(2.34);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(0.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo(1.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo(-2.49);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '123456789123456784'"))
                .isEqualTo(123456789123456784d);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '123456789.123456791'"))
                .isEqualTo(123456789.123456791d);

        assertThat(assertions.expression("cast(a AS DOUBLE)")
                .binding("a", "CAST(DECIMAL '0' as DECIMAL(20, 2))"))
                .isEqualTo(0.0);

        assertThat(assertions.expression("cast(a AS DOUBLE)")
                .binding("a", "CAST(DECIMAL '12.12' as DECIMAL(20, 2))"))
                .isEqualTo(12.12);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .isEqualTo(1234567890.1234567890);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .isEqualTo(-1234567890.1234567890);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '1234567890.12345678900000000000'"))
                .isEqualTo(1234567890.1234567890);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '-1234567890.12345678900000000000'"))
                .isEqualTo(-1234567890.1234567890);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '-1234567890123456789012345678'"))
                .isEqualTo(-1.2345678901234569E27);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(1.0E38);
    }

    @Test
    public void testFloatToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "REAL '234.0'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "REAL '.01'"))
                .isEqualTo(decimal(".010", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "REAL '.0'"))
                .isEqualTo(decimal(".000", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(1,0))")
                .binding("a", "REAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "REAL '0'"))
                .isEqualTo(decimal("0000", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "REAL '1000'"))
                .isEqualTo(decimal("1000", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(7,2))")
                .binding("a", "REAL '1000.01'"))
                .isEqualTo(decimal("01000.01", createDecimalType(7, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,0))")
                .binding("a", "REAL '-234.0'"))
                .isEqualTo(decimal("-234", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "REAL '12345678400000000'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("12345678000000000", createDecimalType(17))
                        : decimal("12345678400000000", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "REAL '-12345678400000000'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-12345678000000000", createDecimalType(17))
                        : decimal("-12345678400000000", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "REAL '1234567940'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("1234568000.0000000000", createDecimalType(20, 10))
                        : decimal("1234567940.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "REAL '-1234567940'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-1234568000.0000000000", createDecimalType(20, 10))
                        : decimal("-1234567940.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "REAL '1234567940'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("1234568000.00000000000000000000", createDecimalType(30, 20))
                        : decimal("1234567940.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "REAL '-1234567940'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-1234568000.00000000000000000000", createDecimalType(30, 20))
                        : decimal("-1234567940.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,0))")
                .binding("a", "REAL '123456790519087104'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("123456790000000000", createDecimalType(18))
                        : decimal("123456791000000000", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,0))")
                .binding("a", "REAL '-123456790519087104'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-123456790000000000", createDecimalType(18))
                        : decimal("-123456791000000000", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,2))")
                .binding("a", "REAL '123456790519087104'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("123456790000000000.00", createDecimalType(20, 2))
                        : decimal("123456791000000000.00", createDecimalType(20, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,2))")
                .binding("a", "REAL '-123456790519087104'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-123456790000000000.00", createDecimalType(20, 2))
                        : decimal("-123456791000000000.00", createDecimalType(20, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,2))")
                .binding("a", "REAL '1234567905190871'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("1234568000000000.00", createDecimalType(18, 2))
                        : decimal("1234567950000000.00", createDecimalType(18, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,2))")
                .binding("a", "REAL '-1234567905190871'"))
                .isEqualTo(Runtime.version().feature() >= 19
                        ? decimal("-1234568000000000.00", createDecimalType(18, 2))
                        : decimal("-1234567950000000.00", createDecimalType(18, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,9))")
                .binding("a", "REAL '1456213.432632456'"))
                .isEqualTo(decimal("001456213.400000000", createDecimalType(18, 9)));

        // test roundtrip
        assertThat(assertions.expression("CAST(CAST(DOUBLE '123456790519087104' AS DECIMAL(18,0)) as DOUBLE) = DOUBLE '123456790519087104'"))
                .isEqualTo(true);

        assertThat(assertions.expression("CAST(CAST(DOUBLE '123456790519087104' AS DECIMAL(30,0)) as DOUBLE) = DOUBLE '123456790519087104'"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(38,37))")
                .binding("a", "REAL '100.02'").evaluate())
                .hasMessage("Cannot cast REAL '100.02' to DECIMAL(38, 37)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(17,16))")
                .binding("a", "REAL '100.02'").evaluate())
                .hasMessage("Cannot cast REAL '100.02' to DECIMAL(17, 16)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "REAL '234.0'").evaluate())
                .hasMessage("Cannot cast REAL '234.0' to DECIMAL(2, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "REAL '1000.01'").evaluate())
                .hasMessage("Cannot cast REAL '1000.01' to DECIMAL(5, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "REAL '-234.0'").evaluate())
                .hasMessage("Cannot cast REAL '-234.0' to DECIMAL(2, 0)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "REAL '98765430784.0'").evaluate())
                .hasMessage(Runtime.version().feature() >= 19
                        ? "Cannot cast REAL '9.876543E10' to DECIMAL(20, 10)"
                        : "Cannot cast REAL '9.8765431E10' to DECIMAL(20, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(10,5))")
                .binding("a", "CAST(nan() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL 'NaN' to DECIMAL(10, 5)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(10,1))")
                .binding("a", "CAST(infinity() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL 'Infinity' to DECIMAL(10, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(1,1))")
                .binding("a", "CAST(-infinity() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL '-Infinity' to DECIMAL(1, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(38,10))")
                .binding("a", "CAST(nan() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL 'NaN' to DECIMAL(38, 10)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(38,2))")
                .binding("a", "CAST(infinity() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL 'Infinity' to DECIMAL(38, 2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS DECIMAL(38,1))")
                .binding("a", "CAST(-infinity() as REAL)").evaluate())
                .hasMessage("Cannot cast REAL '-Infinity' to DECIMAL(38, 1)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToFloatCasts()
    {
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '2.34'"))
                .isEqualTo(2.34f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '0'"))
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '-0'"))
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '1'"))
                .isEqualTo(1.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '-2.49'"))
                .isEqualTo(-2.49f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '123456790519087104'"))
                .isEqualTo(123456790519087104f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '121456.213432632456'"))
                .isEqualTo(121456.21f);

        assertThat(assertions.expression("cast(a AS REAL)")
                .binding("a", "CAST(DECIMAL '0' as DECIMAL(20, 2))"))
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a AS REAL)")
                .binding("a", "CAST(DECIMAL '12.12' as DECIMAL(20, 2))"))
                .isEqualTo(12.12f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .isEqualTo(1234567890.1234567890f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .isEqualTo(-1234567890.1234567890f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '1234567890.12345678900000000000'"))
                .isEqualTo(1234567890.1234567890f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '-1234567890.12345678900000000000'"))
                .isEqualTo(-1234567890.1234567890f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '-1234567890123456789012345678'"))
                .isEqualTo(-1.2345678901234569E27f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "DECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(1.0E38f);
    }

    @Test
    public void testVarcharToDecimalCasts()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4,1))")
                .binding("a", "'234.0'"))
                .isEqualTo(decimal("234.0", createDecimalType(4, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "'.01'"))
                .isEqualTo(decimal(".010", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,3))")
                .binding("a", "'.0'"))
                .isEqualTo(decimal(".000", createDecimalType(3, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(1,0))")
                .binding("a", "'0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "'0'"))
                .isEqualTo(decimal("0000", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(4,0))")
                .binding("a", "'1000'"))
                .isEqualTo(decimal("1000", createDecimalType(4)));

        assertThat(assertions.expression("cast(a as DECIMAL(7,2))")
                .binding("a", "'1000.01'"))
                .isEqualTo(decimal("01000.01", createDecimalType(7, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3,0))")
                .binding("a", "'-234.0'"))
                .isEqualTo(decimal("-234", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "'12345678901234567'"))
                .isEqualTo(decimal("12345678901234567", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,0))")
                .binding("a", "'123456789012345679'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(18)));

        assertThat(assertions.expression("cast(a as DECIMAL(18,8))")
                .binding("a", "'1234567890.12345679'"))
                .isEqualTo(decimal("1234567890.12345679", createDecimalType(18, 8)));

        assertThat(assertions.expression("cast(a as DECIMAL(17,0))")
                .binding("a", "'-12345678901234567'"))
                .isEqualTo(decimal("-12345678901234567", createDecimalType(17)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "'1234567890'"))
                .isEqualTo(decimal("1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(20,10))")
                .binding("a", "'-1234567890'"))
                .isEqualTo(decimal("-1234567890.0000000000", createDecimalType(20, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "'1234567890'"))
                .isEqualTo(decimal("1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertThat(assertions.expression("cast(a as DECIMAL(30,20))")
                .binding("a", "'-1234567890'"))
                .isEqualTo(decimal("-1234567890.00000000000000000000", createDecimalType(30, 20)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "'234.0'").evaluate())
                .hasMessage("Cannot cast VARCHAR '234.0' to DECIMAL(2, 0). Value too large.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(5,2))")
                .binding("a", "'1000.01'").evaluate())
                .hasMessage("Cannot cast VARCHAR '1000.01' to DECIMAL(5, 2). Value too large.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2,0))")
                .binding("a", "'-234.0'").evaluate())
                .hasMessage("Cannot cast VARCHAR '-234.0' to DECIMAL(2, 0). Value too large.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20, 10))")
                .binding("a", "'12345678901'").evaluate())
                .hasMessage("Cannot cast VARCHAR '12345678901' to DECIMAL(20, 10). Value too large.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "'foo'").evaluate())
                .hasMessage("Cannot cast VARCHAR 'foo' to DECIMAL(2, 0). Value is not a number.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL)")
                .binding("a", "'bar'").evaluate())
                .hasMessage("Cannot cast VARCHAR 'bar' to DECIMAL(38, 0). Value is not a number.")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testDecimalToVarcharCasts()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '2.34'"))
                .hasType(VARCHAR)
                .isEqualTo("2.34");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '23400'"))
                .hasType(VARCHAR)
                .isEqualTo("23400");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '0.0034'"))
                .hasType(VARCHAR)
                .isEqualTo("0.0034");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '0'"))
                .hasType(VARCHAR)
                .isEqualTo("0");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '0.1234567890123456'"))
                .hasType(VARCHAR)
                .isEqualTo("0.1234567890123456");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '0.12345678901234567'"))
                .hasType(VARCHAR)
                .isEqualTo("0.12345678901234567");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-10'"))
                .hasType(VARCHAR)
                .isEqualTo("-10");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1.0'"))
                .hasType(VARCHAR)
                .isEqualTo("-1.0");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1.00'"))
                .hasType(VARCHAR)
                .isEqualTo("-1.00");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1.00000'"))
                .hasType(VARCHAR)
                .isEqualTo("-1.00000");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-0.1'"))
                .hasType(VARCHAR)
                .isEqualTo("-0.1");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-.001'"))
                .hasType(VARCHAR)
                .isEqualTo("-0.001");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1234567890.1234567'"))
                .hasType(VARCHAR)
                .isEqualTo("-1234567890.1234567");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '1234567890.1234567890'"))
                .hasType(VARCHAR)
                .isEqualTo("1234567890.1234567890");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1234567890.1234567890'"))
                .hasType(VARCHAR)
                .isEqualTo("-1234567890.1234567890");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '1234567890.12345678900000000000'"))
                .hasType(VARCHAR)
                .isEqualTo("1234567890.12345678900000000000");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "DECIMAL '-1234567890.12345678900000000000'"))
                .hasType(VARCHAR)
                .isEqualTo("-1234567890.12345678900000000000");

        assertThat(assertions.expression("cast(a as varchar(4))")
                .binding("a", "DECIMAL '12.4'"))
                .hasType(createVarcharType(4))
                .isEqualTo("12.4");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "DECIMAL '12.4'"))
                .hasType(createVarcharType(50))
                .isEqualTo("12.4");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(3))")
                .binding("a", "DECIMAL '12.4'").evaluate())
                .hasMessage("Value 12.4 cannot be represented as varchar(3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as varchar(20))")
                .binding("a", "DECIMAL '100000000000000000.1'"))
                .hasType(createVarcharType(20))
                .isEqualTo("100000000000000000.1");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "DECIMAL '100000000000000000.1'"))
                .hasType(createVarcharType(50))
                .isEqualTo("100000000000000000.1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(19))")
                .binding("a", "DECIMAL '100000000000000000.1'").evaluate())
                .hasMessage("Value 100000000000000000.1 cannot be represented as varchar(19)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }
}
