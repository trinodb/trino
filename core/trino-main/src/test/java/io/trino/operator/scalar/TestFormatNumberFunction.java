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

import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestFormatNumberFunction
        extends AbstractTestFunctions
{
    @Test
    public void testFormatNumber()
    {
        assertFunction("format_number(TINYINT '123')", VARCHAR, "123");
        assertFunction("format_number(SMALLINT '12345')", VARCHAR, "12.3K");
        assertFunction("format_number(SMALLINT '12399')", VARCHAR, "12.4K");
        assertFunction("format_number(INTEGER '12345678')", VARCHAR, "12.3M");
        assertFunction("format_number(INTEGER '12399999')", VARCHAR, "12.4M");
        assertFunction("format_number(BIGINT '12345678901')", VARCHAR, "12.3B");
        assertFunction("format_number(BIGINT '12399999999')", VARCHAR, "12.4B");

        assertFunction("format_number(DOUBLE '1234.5')", VARCHAR, "1.23K");
        assertFunction("format_number(DOUBLE '1239.9')", VARCHAR, "1.24K");
        assertFunction("format_number(REAL '1234567.8')", VARCHAR, "1.23M");
        assertFunction("format_number(REAL '1239999.9')", VARCHAR, "1.24M");
        assertFunction("format_number(DECIMAL '1234567890.1')", VARCHAR, "1.23B");
        assertFunction("format_number(DECIMAL '1239999999.9')", VARCHAR, "1.24B");

        assertFunction("format_number(-999)", VARCHAR, "-999");
        assertFunction("format_number(-1000)", VARCHAR, "-1K");
        assertFunction("format_number(-999999)", VARCHAR, "-1000K");
        assertFunction("format_number(-1000000)", VARCHAR, "-1M");
        assertFunction("format_number(-999999999)", VARCHAR, "-1000M");
        assertFunction("format_number(-1000000000)", VARCHAR, "-1B");
        assertFunction("format_number(-999999999999)", VARCHAR, "-1000B");
        assertFunction("format_number(-1000000000000)", VARCHAR, "-1T");
        assertFunction("format_number(-999999999999999)", VARCHAR, "-1000T");
        assertFunction("format_number(-1000000000000000)", VARCHAR, "-1Q");
        assertFunction("format_number(-9223372036854775808)", VARCHAR, "-9223.37Q");

        assertFunction("format_number(0)", VARCHAR, "0");
        assertFunction("format_number(999)", VARCHAR, "999");
        assertFunction("format_number(1000)", VARCHAR, "1K");
        assertFunction("format_number(999999)", VARCHAR, "1000K");
        assertFunction("format_number(1000000)", VARCHAR, "1M");
        assertFunction("format_number(999999999)", VARCHAR, "1000M");
        assertFunction("format_number(1000000000)", VARCHAR, "1B");
        assertFunction("format_number(999999999999)", VARCHAR, "1000B");
        assertFunction("format_number(1000000000000)", VARCHAR, "1T");
        assertFunction("format_number(999999999999999)", VARCHAR, "1000T");
        assertFunction("format_number(1000000000000000)", VARCHAR, "1Q");
        assertFunction("format_number(9223372036854775807)", VARCHAR, "9223Q");

        assertFunction("format_number(CAST(NULL AS TINYINT))", VARCHAR, null);
        assertFunction("format_number(CAST(NULL AS SMALLINT))", VARCHAR, null);
        assertFunction("format_number(CAST(NULL AS INTEGER))", VARCHAR, null);
        assertFunction("format_number(CAST(NULL AS DOUBLE))", VARCHAR, null);
        assertFunction("format_number(CAST(NULL AS REAL))", VARCHAR, null);
        assertFunction("format_number(CAST(NULL AS DECIMAL))", VARCHAR, null);
    }
}
