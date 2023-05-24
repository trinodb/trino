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

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDecimalParametricType
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
    public void decimalIsCreatedWithPrecisionAndScaleDefined()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(2, 0))")
                .binding("a", "1"))
                .isEqualTo(decimal("01", createDecimalType(2, 0)));

        assertThat(assertions.expression("cast(a as DECIMAL(2, 2))")
                .binding("a", "0.01"))
                .isEqualTo(decimal(".01", createDecimalType(2, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(10, 10))")
                .binding("a", "0.02"))
                .isEqualTo(decimal(".0200000000", createDecimalType(10, 10)));

        assertThat(assertions.expression("cast(a as DECIMAL(10, 8))")
                .binding("a", "0.02"))
                .isEqualTo(decimal("00.02000000", createDecimalType(10, 8)));
    }

    @Test
    public void decimalIsCreatedWithOnlyPrecisionDefined()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(2))")
                .binding("a", "1"))
                .isEqualTo(decimal("01", createDecimalType(2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3))")
                .binding("a", "-22"))
                .isEqualTo(decimal("-022", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(4))")
                .binding("a", "31.41"))
                .isEqualTo(decimal("0031", createDecimalType(4)));
    }

    @Test
    public void decimalIsCreatedWithoutParameters()
    {
        assertThat(assertions.expression("cast(a as DECIMAL)")
                .binding("a", "1"))
                .isEqualTo(decimal("1", createDecimalType(38)));

        assertThat(assertions.expression("cast(a as DECIMAL)")
                .binding("a", "-22"))
                .isEqualTo(decimal("-22", createDecimalType(38)));

        assertThat(assertions.expression("cast(a as DECIMAL)")
                .binding("a", "31.41"))
                .isEqualTo(decimal("31", createDecimalType(38)));
    }

    @Test
    public void creatingDecimalRoundsValueProperly()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(4, 2))")
                .binding("a", "0.022"))
                .isEqualTo(decimal("00.02", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4, 2))")
                .binding("a", "0.025"))
                .isEqualTo(decimal("00.03", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 1))")
                .binding("a", "32.01"))
                .isEqualTo(decimal("32.0", createDecimalType(3, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 1))")
                .binding("a", "32.06"))
                .isEqualTo(decimal("32.1", createDecimalType(3, 1)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 0))")
                .binding("a", "32.1"))
                .isEqualTo(decimal("032", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(3, 0))")
                .binding("a", "32.5"))
                .isEqualTo(decimal("033", createDecimalType(3)));

        assertThat(assertions.expression("cast(a as DECIMAL(4, 2))")
                .binding("a", "-0.022"))
                .isEqualTo(decimal("-00.02", createDecimalType(4, 2)));

        assertThat(assertions.expression("cast(a as DECIMAL(4, 2))")
                .binding("a", "-0.025"))
                .isEqualTo(decimal("-00.03", createDecimalType(4, 2)));
    }

    @Test
    public void decimalIsNotCreatedWhenScaleExceedsPrecision()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(1,2))")
                .binding("a", "1").evaluate())
                .hasMessage("DECIMAL scale must be in range [0, precision (1)]: 2");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(20,21))")
                .binding("a", "-22").evaluate())
                .hasMessage("DECIMAL scale must be in range [0, precision (20)]: 21");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(0,1))")
                .binding("a", "31.41").evaluate())
                .hasMessage("DECIMAL precision must be in range [1, 38]: 0");
    }

    @Test
    public void decimalWithZeroLengthCannotBeCreated()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(0,0))")
                .binding("a", "1").evaluate())
                .hasMessage("DECIMAL precision must be in range [1, 38]: 0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(0,0))")
                .binding("a", "0").evaluate())
                .hasMessage("DECIMAL precision must be in range [1, 38]: 0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(0))")
                .binding("a", "1").evaluate())
                .hasMessage("DECIMAL precision must be in range [1, 38]: 0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(0))")
                .binding("a", "0").evaluate())
                .hasMessage("DECIMAL precision must be in range [1, 38]: 0");
    }
}
