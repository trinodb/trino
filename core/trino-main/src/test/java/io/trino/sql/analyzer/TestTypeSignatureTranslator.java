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
package io.trino.sql.analyzer;

import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.Locale;

import static io.trino.sql.analyzer.TypeSignatureTranslator.toDataType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.parser.ParserAssert.type;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeSignatureTranslator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private void assertRoundTrip(String expression)
    {
        assertThat(type(expression))
                .ignoringLocation()
                .withComparatorForType(Comparator.comparing(identifier -> identifier.getValue().toLowerCase(Locale.ENGLISH)), Identifier.class)
                .isEqualTo(toDataType(toTypeSignature(SQL_PARSER.createType(expression))));
    }

    @Test
    public void testSimpleTypes()
    {
        assertRoundTrip("varchar");
        assertRoundTrip("BIGINT");
        assertRoundTrip("DOUBLE");
        assertRoundTrip("BOOLEAN");
    }

    @Test
    public void testDayTimeTypes()
    {
        assertRoundTrip("TIMESTAMP");
        assertRoundTrip("TIMESTAMP(1)");
        assertRoundTrip("TIMESTAMP WITHOUT TIME ZONE");
        assertRoundTrip("TIMESTAMP(1) WITHOUT TIME ZONE");
        assertRoundTrip("TIMESTAMP WITH TIME ZONE");
        assertRoundTrip("TIME");
        assertRoundTrip("TIME WITHOUT TIME ZONE");
        assertRoundTrip("TIME WITH TIME ZONE");
    }

    @Test
    public void testIntervalTypes()
    {
        assertRoundTrip("INTERVAL DAY TO SECOND");
        assertRoundTrip("INTERVAL YEAR TO MONTH");
    }

    @Test
    public void testParametricTypes()
    {
        assertRoundTrip("ARRAY(TINYINT)");
        assertRoundTrip("MAP(BIGINT, SMALLINT)");
        assertRoundTrip("VARCHAR(123)");
        assertRoundTrip("DECIMAL(1)");
        assertRoundTrip("DECIMAL(1, 38)");
    }

    @Test
    public void testArray()
    {
        assertRoundTrip("foo(42, 55) ARRAY");
        assertRoundTrip("VARCHAR(7) ARRAY");
        assertRoundTrip("VARCHAR(7) ARRAY array");
    }

    @Test
    public void testRowType()
    {
        assertRoundTrip("ROW(a BIGINT, b VARCHAR)");
        assertRoundTrip("ROW(a BIGINT,b VARCHAR)");
        assertRoundTrip("ROW(\"a\" BIGINT, \"from\" VARCHAR)");
        assertRoundTrip("ROW(\"$x\" BIGINT, \"$y\" VARCHAR)");
        assertRoundTrip("ROW(\"ident with spaces\" BIGINT)");
    }

    @Test
    public void testComplexTypes()
    {
        assertRoundTrip("ROW(x BIGINT, y DOUBLE PRECISION, z ROW(m array<bigint>,n map<double,varchar>))");
    }
}
