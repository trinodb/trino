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

import io.trino.spi.type.TypeSignature;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Locale;
import java.util.Set;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.toDataType;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toTypeDescriptor;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.sql.parser.ParserAssert.type;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeDescriptorTranslator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private void assertRoundTrip(String expression)
    {
        assertThat(type(expression))
                .ignoringLocation()
                .withComparatorForType(Comparator.comparing(identifier -> identifier.getValue().toLowerCase(Locale.ENGLISH)), Identifier.class)
                .isEqualTo(toDataType(toTypeDescriptor(SQL_PARSER.createType(expression))));
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

    @Test
    public void testParseTypeSignaturePreservesRowFieldNameCase()
    {
        // Regression test for a cache poisoning bug where DATA_TYPE_CACHE was keyed by the lowercased signature
        // while storing the DataType parsed from the original-cased signature. The first case-variant of a row
        // signature would then win for every later lookup, returning field names with the wrong casing.
        // Downstream, NamedTypeSignature.equals is case-sensitive on field names, so function dependency
        // resolution would miss and throw UndeclaredDependencyException at filter compile time.
        TypeSignature camelCaseFirst = parseTypeSignature("row(\"memberId\" integer, \"viewerUrn\" varchar)", Set.of());
        TypeSignature lowerCaseAfter = parseTypeSignature("row(\"memberid\" integer, \"viewerurn\" varchar)", Set.of());

        assertThat(camelCaseFirst.toString())
                .isEqualTo("row(\"memberId\" integer,\"viewerUrn\" varchar)");
        assertThat(lowerCaseAfter.toString())
                .isEqualTo("row(\"memberid\" integer,\"viewerurn\" varchar)");
        assertThat(camelCaseFirst).isNotEqualTo(lowerCaseAfter);

        // Reverse order: lowercase first, camelCase second. Both must still preserve their input casing.
        TypeSignature lowerCaseFirst = parseTypeSignature("row(\"actorurn\" varchar, \"appname\" varchar)", Set.of());
        TypeSignature camelCaseAfter = parseTypeSignature("row(\"actorUrn\" varchar, \"appName\" varchar)", Set.of());

        assertThat(lowerCaseFirst.toString())
                .isEqualTo("row(\"actorurn\" varchar,\"appname\" varchar)");
        assertThat(camelCaseAfter.toString())
                .isEqualTo("row(\"actorUrn\" varchar,\"appName\" varchar)");
        assertThat(lowerCaseFirst).isNotEqualTo(camelCaseAfter);
    }
}
