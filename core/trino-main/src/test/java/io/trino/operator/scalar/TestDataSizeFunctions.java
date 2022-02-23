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
import static io.trino.spi.type.VarcharType.VARCHAR;
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

    @Test
    public void testParseDataSizeDecimal()
    {
        assertThat(assertions.function("parse_data_size_decimal", "'0B'"))
                .isEqualTo(decimal("0", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'1B'"))
                .isEqualTo(decimal("1", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'1.2B'"))
                .isEqualTo(decimal("1", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'1.9B'"))
                .isEqualTo(decimal("1", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'2.2kB'"))
                .isEqualTo(decimal("2200", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'2.23kB'"))
                .isEqualTo(decimal("2230", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'2.234kB'"))
                .isEqualTo(decimal("2234", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'3MB'"))
                .isEqualTo(decimal("3000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'4GB'"))
                .isEqualTo(decimal("4000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'4TB'"))
                .isEqualTo(decimal("4000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'5PB'"))
                .isEqualTo(decimal("5000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'6EB'"))
                .isEqualTo(decimal("6000000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'7ZB'"))
                .isEqualTo(decimal("7000000000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'8YB'"))
                .isEqualTo(decimal("8000000000000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'6917529027641081856EB'"))
                .isEqualTo(decimal("6917529027641081856000000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'69175290276410818560EB'"))
                .isEqualTo(decimal("69175290276410818560000000000000000000", DECIMAL));
        assertThat(assertions.function("parse_data_size_decimal", "'99999999999999YB'"))
                .isEqualTo(decimal("99999999999999000000000000000000000000", DECIMAL));

        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "''").evaluate())
                .hasMessage("Invalid data size: ''");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'0'").evaluate())
                .hasMessage("Invalid data size: '0'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'10KB'").evaluate())
                .hasMessage("Invalid data size: '10KB'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'KB'").evaluate())
                .hasMessage("Invalid data size: 'KB'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'-1B'").evaluate())
                .hasMessage("Invalid data size: '-1B'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'12345K'").evaluate())
                .hasMessage("Invalid data size: '12345K'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'A12345B'").evaluate())
                .hasMessage("Invalid data size: 'A12345B'");
        assertTrinoExceptionThrownBy(() -> assertions.function("parse_data_size_decimal", "'999999999999999YB'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Value out of range: '999999999999999YB' ('999999999999999000000000000000000000000B')");
    }

    @Test
    public void testFormatDataSize()
    {
        assertThat(assertions.function("format_data_size", "TINYINT '123'"))
                .isEqualTo("123B");
        assertThat(assertions.function("format_data_size", "SMALLINT '12345'"))
                .isEqualTo("12.1kB");
        assertThat(assertions.function("format_data_size", "SMALLINT '12399'"))
                .isEqualTo("12.1kB");
        assertThat(assertions.function("format_data_size", "INTEGER '123456'"))
                .isEqualTo("121kB");
        assertThat(assertions.function("format_data_size", "INTEGER '1048576'"))
                .isEqualTo("1MB");
        assertThat(assertions.function("format_data_size", "INTEGER '12345678'"))
                .isEqualTo("11.8MB");
        assertThat(assertions.function("format_data_size", "INTEGER '12399999'"))
                .isEqualTo("11.8MB");
        assertThat(assertions.function("format_data_size", "BIGINT '12345678901'"))
                .isEqualTo("11.5GB");
        assertThat(assertions.function("format_data_size", "BIGINT '12399999999'"))
                .isEqualTo("11.5GB");

        assertThat(assertions.function("format_data_size", "DOUBLE '1234.5'"))
                .isEqualTo("1.21kB");
        assertThat(assertions.function("format_data_size", "DOUBLE '1239.9'"))
                .isEqualTo("1.21kB");
        assertThat(assertions.function("format_data_size", "REAL '1234567.8'"))
                .isEqualTo("1.18MB");
        assertThat(assertions.function("format_data_size", "REAL '1239999.9'"))
                .isEqualTo("1.18MB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1234567890.1'"))
                .isEqualTo("1.15GB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999.9'"))
                .isEqualTo("1.15GB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999.9'"))
                .isEqualTo("1.13TB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999999.9'"))
                .isEqualTo("1.1PB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999999999.9'"))
                .isEqualTo("1.08EB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999999999999.9'"))
                .isEqualTo("1.05ZB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999999999999999.9'"))
                .isEqualTo("1.03YB");
        assertThat(assertions.function("format_data_size", "DECIMAL '1239999999999999999999999999.9'"))
                .isEqualTo("1026YB");

        assertThat(assertions.function("format_data_size", "-999"))
                .isEqualTo("-999B");
        assertThat(assertions.function("format_data_size", "-1000"))
                .isEqualTo("-1000B");
        assertThat(assertions.function("format_data_size", "-999999"))
                .isEqualTo("-977kB");
        assertThat(assertions.function("format_data_size", "-1000000"))
                .isEqualTo("-977kB");
        assertThat(assertions.function("format_data_size", "-999999999"))
                .isEqualTo("-954MB");
        assertThat(assertions.function("format_data_size", "-1000000000"))
                .isEqualTo("-954MB");
        assertThat(assertions.function("format_data_size", "-999999999999"))
                .isEqualTo("-931GB");
        assertThat(assertions.function("format_data_size", "-1000000000000"))
                .isEqualTo("-931GB");
        assertThat(assertions.function("format_data_size", "-999999999999999"))
                .isEqualTo("-909TB");
        assertThat(assertions.function("format_data_size", "-1000000000000000"))
                .isEqualTo("-909TB");
        assertThat(assertions.function("format_data_size", "-9223372036854775808"))
                .isEqualTo("-8EB");

        assertThat(assertions.function("format_data_size", "0"))
                .isEqualTo("0B");
        assertThat(assertions.function("format_data_size", "999"))
                .isEqualTo("999B");
        assertThat(assertions.function("format_data_size", "1000"))
                .isEqualTo("1000B");
        assertThat(assertions.function("format_data_size", "999999"))
                .isEqualTo("977kB");
        assertThat(assertions.function("format_data_size", "1000000"))
                .isEqualTo("977kB");
        assertThat(assertions.function("format_data_size", "999999999"))
                .isEqualTo("954MB");
        assertThat(assertions.function("format_data_size", "1000000000"))
                .isEqualTo("954MB");
        assertThat(assertions.function("format_data_size", "999999999999"))
                .isEqualTo("931GB");
        assertThat(assertions.function("format_data_size", "1000000000000"))
                .isEqualTo("931GB");
        assertThat(assertions.function("format_data_size", "999999999999999"))
                .isEqualTo("909TB");
        assertThat(assertions.function("format_data_size", "1000000000000000"))
                .isEqualTo("909TB");
        assertThat(assertions.function("format_data_size", "9223372036854775807"))
                .isEqualTo("8EB");

        assertThat(assertions.function("format_data_size", "DOUBLE 'NaN'"))
                .isEqualTo("NaN");
        assertThat(assertions.function("format_data_size", "DOUBLE '+Infinity'"))
                .isEqualTo("Infinity");
        assertThat(assertions.function("format_data_size", "DOUBLE '-Infinity'"))
                .isEqualTo("-Infinity");

        assertThat(assertions.function("format_data_size", "CAST(NULL AS TINYINT)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size", "CAST(NULL AS SMALLINT)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size", "CAST(NULL AS INTEGER)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size", "CAST(NULL AS DOUBLE)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size", "CAST(NULL AS REAL)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size", "CAST(NULL AS DECIMAL)"))
                .isNull(VARCHAR);
    }

    @Test
    public void testFormatDataSizeDecimal()
    {
        assertThat(assertions.function("format_data_size_decimal", "TINYINT '123'"))
                .isEqualTo("123B");
        assertThat(assertions.function("format_data_size_decimal", "SMALLINT '12345'"))
                .isEqualTo("12.3kB");
        assertThat(assertions.function("format_data_size_decimal", "SMALLINT '12399'"))
                .isEqualTo("12.4kB");
        assertThat(assertions.function("format_data_size_decimal", "INTEGER '123456'"))
                .isEqualTo("123kB");
        assertThat(assertions.function("format_data_size_decimal", "INTEGER '1048576'"))
                .isEqualTo("1.05MB");
        assertThat(assertions.function("format_data_size_decimal", "INTEGER '12345678'"))
                .isEqualTo("12.3MB");
        assertThat(assertions.function("format_data_size_decimal", "INTEGER '12399999'"))
                .isEqualTo("12.4MB");
        assertThat(assertions.function("format_data_size_decimal", "BIGINT '12345678901'"))
                .isEqualTo("12.3GB");
        assertThat(assertions.function("format_data_size_decimal", "BIGINT '12399999999'"))
                .isEqualTo("12.4GB");

        assertThat(assertions.function("format_data_size_decimal", "DOUBLE '1234.5'"))
                .isEqualTo("1.23kB");
        assertThat(assertions.function("format_data_size_decimal", "DOUBLE '1239.9'"))
                .isEqualTo("1.24kB");
        assertThat(assertions.function("format_data_size_decimal", "REAL '1234567.8'"))
                .isEqualTo("1.23MB");
        assertThat(assertions.function("format_data_size_decimal", "REAL '1239999.9'"))
                .isEqualTo("1.24MB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1234567890.1'"))
                .isEqualTo("1.23GB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999.9'"))
                .isEqualTo("1.24GB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999.9'"))
                .isEqualTo("1.24TB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999999.9'"))
                .isEqualTo("1.24PB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999999999.9'"))
                .isEqualTo("1.24EB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999999999999.9'"))
                .isEqualTo("1.24ZB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999999999999999.9'"))
                .isEqualTo("1.24YB");
        assertThat(assertions.function("format_data_size_decimal", "DECIMAL '1239999999999999999999999999.9'"))
                .isEqualTo("1240YB");

        assertThat(assertions.function("format_data_size_decimal", "-999"))
                .isEqualTo("-999B");
        assertThat(assertions.function("format_data_size_decimal", "-1000"))
                .isEqualTo("-1kB");
        assertThat(assertions.function("format_data_size_decimal", "-999999"))
                .isEqualTo("-1000kB");
        assertThat(assertions.function("format_data_size_decimal", "-1000000"))
                .isEqualTo("-1MB");
        assertThat(assertions.function("format_data_size_decimal", "-999999999"))
                .isEqualTo("-1000MB");
        assertThat(assertions.function("format_data_size_decimal", "-1000000000"))
                .isEqualTo("-1GB");
        assertThat(assertions.function("format_data_size_decimal", "-999999999999"))
                .isEqualTo("-1000GB");
        assertThat(assertions.function("format_data_size_decimal", "-1000000000000"))
                .isEqualTo("-1TB");
        assertThat(assertions.function("format_data_size_decimal", "-999999999999999"))
                .isEqualTo("-1000TB");
        assertThat(assertions.function("format_data_size_decimal", "-1000000000000000"))
                .isEqualTo("-1PB");
        assertThat(assertions.function("format_data_size_decimal", "-9223372036854775808"))
                .isEqualTo("-9.22EB");

        assertThat(assertions.function("format_data_size_decimal", "0"))
                .isEqualTo("0B");
        assertThat(assertions.function("format_data_size_decimal", "999"))
                .isEqualTo("999B");
        assertThat(assertions.function("format_data_size_decimal", "1000"))
                .isEqualTo("1kB");
        assertThat(assertions.function("format_data_size_decimal", "999999"))
                .isEqualTo("1000kB");
        assertThat(assertions.function("format_data_size_decimal", "1000000"))
                .isEqualTo("1MB");
        assertThat(assertions.function("format_data_size_decimal", "999999999"))
                .isEqualTo("1000MB");
        assertThat(assertions.function("format_data_size_decimal", "1000000000"))
                .isEqualTo("1GB");
        assertThat(assertions.function("format_data_size_decimal", "999999999999"))
                .isEqualTo("1000GB");
        assertThat(assertions.function("format_data_size_decimal", "1000000000000"))
                .isEqualTo("1TB");
        assertThat(assertions.function("format_data_size_decimal", "999999999999999"))
                .isEqualTo("1000TB");
        assertThat(assertions.function("format_data_size_decimal", "1000000000000000"))
                .isEqualTo("1PB");
        assertThat(assertions.function("format_data_size_decimal", "9223372036854775807"))
                .isEqualTo("9.22EB");

        assertThat(assertions.function("format_data_size_decimal", "DOUBLE 'NaN'"))
                .isEqualTo("NaN");
        assertThat(assertions.function("format_data_size_decimal", "DOUBLE '+Infinity'"))
                .isEqualTo("Infinity");
        assertThat(assertions.function("format_data_size_decimal", "DOUBLE '-Infinity'"))
                .isEqualTo("-Infinity");

        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS TINYINT)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS SMALLINT)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS INTEGER)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS DOUBLE)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS REAL)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_data_size_decimal", "CAST(NULL AS DECIMAL)"))
                .isNull(VARCHAR);
    }
}
