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
package io.trino.operator.scalar;

import io.trino.spi.type.DecimalType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataSizeFunctions
{
    private static final DecimalType DECIMAL = createDecimalType(38, 0);

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
    public void testParseDataSize()
    {
        assertThat(assertions.function("parse_data_size", "'0B'"))
                .isEqualTo(decimal("0", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'1B'"))
                .isEqualTo(decimal("1", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'1.2B'"))
                .isEqualTo(decimal("1", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'1.9B'"))
                .isEqualTo(decimal("1", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'2.2kB'"))
                .isEqualTo(decimal("2252", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'2.23kB'"))
                .isEqualTo(decimal("2283", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'2.234kB'"))
                .isEqualTo(decimal("2287", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'3MB'"))
                .isEqualTo(decimal("3145728", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'4GB'"))
                .isEqualTo(decimal("4294967296", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'4TB'"))
                .isEqualTo(decimal("4398046511104", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'5PB'"))
                .isEqualTo(decimal("5629499534213120", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'6EB'"))
                .isEqualTo(decimal("6917529027641081856", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'7ZB'"))
                .isEqualTo(decimal("8264141345021879123968", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'8YB'"))
                .isEqualTo(decimal("9671406556917033397649408", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'6917529027641081856EB'"))
                .isEqualTo(decimal("7975367974709495237422842361682067456", DECIMAL));

        assertThat(assertions.function("parse_data_size", "'69175290276410818560EB'"))
                .isEqualTo(decimal("79753679747094952374228423616820674560", DECIMAL));

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "''").evaluate())
                .hasMessage("Invalid data size: ''");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'0'").evaluate())
                .hasMessage("Invalid data size: '0'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'10KB'").evaluate())
                .hasMessage("Invalid data size: '10KB'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'KB'").evaluate())
                .hasMessage("Invalid data size: 'KB'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'-1B'").evaluate())
                .hasMessage("Invalid data size: '-1B'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'12345K'").evaluate())
                .hasMessage("Invalid data size: '12345K'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'A12345B'").evaluate())
                .hasMessage("Invalid data size: 'A12345B'");

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size", "'99999999999999YB'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range: '99999999999999YB' ('120892581961461708544797985370825293824B')");
    }
}
