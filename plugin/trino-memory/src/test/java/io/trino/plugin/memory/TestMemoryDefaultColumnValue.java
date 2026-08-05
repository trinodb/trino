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
package io.trino.plugin.memory;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryDefaultColumnValue
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder().build();
    }

    @Test
    void testNull()
    {
        assertDefaultValue("TINYINT", "null", null);
    }

    @Test
    void testBoolean()
    {
        assertDefaultValue("BOOLEAN", "true", "true");
        assertDefaultValue("BOOLEAN", "false", "false");
    }

    @Test
    void testTinyint()
    {
        assertDefaultValue("TINYINT", "-128", "TINYINT '-128'");
        assertDefaultValue("TINYINT", "127", "TINYINT '127'");
    }

    @Test
    void testSmallint()
    {
        assertDefaultValue("SMALLINT", "-32768", "SMALLINT '-32768'");
        assertDefaultValue("SMALLINT", "32767", "SMALLINT '32767'");
    }

    @Test
    void testInteger()
    {
        assertDefaultValue("INTEGER", "-2147483648", "-2147483648");
        assertDefaultValue("INTEGER", "2147483647", "2147483647");
    }

    @Test
    void testBigint()
    {
        assertDefaultValue("BIGINT", "-9223372036854775808", "-9223372036854775808");
        assertDefaultValue("BIGINT", "9223372036854775807", "9223372036854775807");
    }

    @Test
    void testReal()
    {
        assertDefaultValue("REAL", "REAL '3.14'");
        assertDefaultValue("REAL", "REAL '10.3e0'");
        assertDefaultValue("REAL", "123", "REAL '123'");

        assertDefaultValue("REAL", "REAL 'NaN'");
        assertDefaultValue("REAL", "REAL '+Infinity'");
        assertDefaultValue("REAL", "REAL '-Infinity'");
    }

    @Test
    void testDouble()
    {
        assertDefaultValue("DOUBLE", "3.14", "3.14E0");
        assertDefaultValue("DOUBLE", "1.0E100", "1.0E100");
        assertDefaultValue("DOUBLE", "1.23456E12", "1.23456E12");
        assertDefaultValue("DOUBLE", "123", "1.23E2");

        assertDefaultValue("DOUBLE", "DOUBLE 'NaN'");
        assertDefaultValue("DOUBLE", "DOUBLE '+Infinity'");
        assertDefaultValue("DOUBLE", "DOUBLE '-Infinity'");
    }

    @Test
    void testDecimal()
    {
        assertDefaultValue("DECIMAL(3)", "193", "DECIMAL '193'");
        assertDefaultValue("DECIMAL(3)", "-193", "DECIMAL '-193'");
        assertDefaultValue("DECIMAL(3, 1)", "10.0", "DECIMAL '10.0'");
        assertDefaultValue("DECIMAL(3, 1)", "-10.1", "DECIMAL '-10.1'");
        assertDefaultValue("DECIMAL(30, 5)", "3141592653589793238462643.38327", "DECIMAL '3141592653589793238462643.38327'");
        assertDefaultValue("DECIMAL(30, 5)", "-3141592653589793238462643.38327", "DECIMAL '-3141592653589793238462643.38327'");
        assertDefaultValue("DECIMAL(38)", "DECIMAL '27182818284590452353602874713526624977'");
        assertDefaultValue("DECIMAL(38)", "DECIMAL '-27182818284590452353602874713526624977'");
    }

    @Test
    void testChar()
    {
        assertDefaultValue("CHAR(4)", "'test'", "'test'");
        assertDefaultValue("CHAR(10)", "'test'", "'test      '");
        assertDefaultValue("CHAR(5)", "'攻殻機動隊'", "'攻殻機動隊'");
        assertDefaultValue("CHAR(1)", "'😂'", "'😂'");
    }

    @Test
    void testVarchar()
    {
        assertDefaultValue("VARCHAR(4)", "'test'", "'test'");
        assertDefaultValue("VARCHAR(10)", "'test'", "'test'");
        assertDefaultValue("VARCHAR", "'test'", "'test'");
        assertDefaultValue("VARCHAR(5)", "'攻殻機動隊'", "'攻殻機動隊'");
        assertDefaultValue("VARCHAR(1)", "'😂'", "'😂'");
    }

    @Test
    void testTime()
    {
        assertDefaultValue("TIME(0)", "TIME '00:00:01'");

        assertDefaultValue("TIME(0)", "TIME '00:00:00'");
        assertDefaultValue("TIME(1)", "TIME '00:00:00.1'");
        assertDefaultValue("TIME(2)", "TIME '00:00:00.12'");
        assertDefaultValue("TIME(3)", "TIME '00:00:00.123'");
        assertDefaultValue("TIME(4)", "TIME '00:00:00.1234'");
        assertDefaultValue("TIME(5)", "TIME '00:00:00.12345'");
        assertDefaultValue("TIME(6)", "TIME '00:00:00.123456'");
        assertDefaultValue("TIME(7)", "TIME '00:00:00.1234567'");
        assertDefaultValue("TIME(8)", "TIME '00:00:00.12345678'");
        assertDefaultValue("TIME(9)", "TIME '00:00:00.123456789'");
        assertDefaultValue("TIME(10)", "TIME '00:00:00.1234567890'");
        assertDefaultValue("TIME(11)", "TIME '00:00:00.12345678901'");
        assertDefaultValue("TIME(12)", "TIME '00:00:00.123456789012'");
    }

    @Test
    void testDate()
    {
        assertDefaultValue("DATE", "DATE '0001-01-01'", "DATE '0001-01-01'");
        assertDefaultValue("DATE", "DATE '1969-12-31'", "DATE '1969-12-31'");
        assertDefaultValue("DATE", "DATE '1970-01-01'", "DATE '1970-01-01'");
        assertDefaultValue("DATE", "DATE '9999-12-31'", "DATE '9999-12-31'");
    }

    @Test
    void testTimestamp()
    {
        assertDefaultValue("TIMESTAMP(0)", "TIMESTAMP '1970-01-01 00:00:00'");
        assertDefaultValue("TIMESTAMP(1)", "TIMESTAMP '1970-01-01 00:00:00.9'");
        assertDefaultValue("TIMESTAMP(2)", "TIMESTAMP '1970-01-01 00:00:00.99'");
        assertDefaultValue("TIMESTAMP(3)", "TIMESTAMP '1970-01-01 00:00:00.999'");
        assertDefaultValue("TIMESTAMP(4)", "TIMESTAMP '1970-01-01 00:00:00.9999'");
        assertDefaultValue("TIMESTAMP(5)", "TIMESTAMP '1970-01-01 00:00:00.99999'");
        assertDefaultValue("TIMESTAMP(6)", "TIMESTAMP '1970-01-01 00:00:00.999999'");
        assertDefaultValue("TIMESTAMP(7)", "TIMESTAMP '1970-01-01 00:00:00.9999999'");
        assertDefaultValue("TIMESTAMP(8)", "TIMESTAMP '1970-01-01 00:00:00.99999999'");
        assertDefaultValue("TIMESTAMP(9)", "TIMESTAMP '1970-01-01 00:00:00.999999999'");
        assertDefaultValue("TIMESTAMP(10)", "TIMESTAMP '1970-01-01 00:00:00.9999999999'");
        assertDefaultValue("TIMESTAMP(11)", "TIMESTAMP '1970-01-01 00:00:00.99999999999'");
        assertDefaultValue("TIMESTAMP(12)", "TIMESTAMP '1970-01-01 00:00:00.999999999999'");
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertDefaultValue("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00 UTC'");
        assertDefaultValue("TIMESTAMP(1) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.9 UTC'");
        assertDefaultValue("TIMESTAMP(2) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.99 UTC'");
        assertDefaultValue("TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.999 UTC'");
        assertDefaultValue("TIMESTAMP(4) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.9999 UTC'");
        assertDefaultValue("TIMESTAMP(5) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.99999 UTC'");
        assertDefaultValue("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.999999 UTC'");
        assertDefaultValue("TIMESTAMP(7) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.9999999 UTC'");
        assertDefaultValue("TIMESTAMP(8) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.99999999 UTC'");
        assertDefaultValue("TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.999999999 UTC'");
        assertDefaultValue("TIMESTAMP(10) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.9999999999 UTC'");
        assertDefaultValue("TIMESTAMP(11) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.99999999999 UTC'");
        assertDefaultValue("TIMESTAMP(12) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.999999999999 UTC'");
    }

    @Test
    void testInformationSchema()
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data int DEFAULT 123)")) {
            assertThat((String) computeScalar("SELECT column_default FROM information_schema.columns WHERE table_name = '" + table.getName() + "' AND column_name = 'data'"))
                    .isEqualTo("123");
        }
    }

    private void assertDefaultValue(@Language("SQL") String columnType, @Language("SQL") String defaultValue)
    {
        assertDefaultValue(columnType, defaultValue, defaultValue);
    }

    private void assertDefaultValue(@Language("SQL") String columnType, @Language("SQL") String literal, @Language("SQL") String expected)
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data " + columnType + " DEFAULT " + literal + ")")) {
            assertUpdate("INSERT INTO " + table.getName() + " (id) VALUES (1)", 1);
            assertThat(query("SELECT data FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " + expected);
        }

        try (TestTable table = newTrinoTable("test_default_value", "(id int, data " + columnType + ")")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN data SET DEFAULT " + literal);
            assertUpdate("INSERT INTO " + table.getName() + " (id) VALUES (1)", 1);
            assertThat(query("SELECT data FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " + expected);

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN data DROP DEFAULT");
            assertUpdate("INSERT INTO " + table.getName() + " (id) VALUES (2)", 1);
            assertThat(query("SELECT data FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " + expected + ", NULL");
        }
    }
}
