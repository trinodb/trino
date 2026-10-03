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

import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeSyntax;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toDataType;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.toTypeDescriptor;
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
        // a bare leading field carries the SQL-spec implicit precision of 2, and a trailing SECOND the
        // implicit fractional-seconds precision of 6, so the round-trip materializes both explicitly
        assertRoundTrip("INTERVAL YEAR(2)");
        assertRoundTrip("INTERVAL MONTH(2)");
        assertRoundTrip("INTERVAL YEAR(2) TO MONTH");
        assertRoundTrip("INTERVAL DAY(2)");
        assertRoundTrip("INTERVAL HOUR(2)");
        assertRoundTrip("INTERVAL MINUTE(2)");
        assertRoundTrip("INTERVAL SECOND(2, 6)");
        assertRoundTrip("INTERVAL DAY(2) TO HOUR");
        assertRoundTrip("INTERVAL DAY(2) TO MINUTE");
        assertRoundTrip("INTERVAL DAY(2) TO SECOND(6)");
        assertRoundTrip("INTERVAL HOUR(2) TO MINUTE");
        assertRoundTrip("INTERVAL HOUR(2) TO SECOND(6)");
        assertRoundTrip("INTERVAL MINUTE(2) TO SECOND(6)");

        // an explicit leading-field and fractional-seconds precision round-trips
        assertRoundTrip("INTERVAL DAY(3)");
        assertRoundTrip("INTERVAL YEAR(4) TO MONTH");
        assertRoundTrip("INTERVAL DAY(5) TO SECOND(6)");
        assertRoundTrip("INTERVAL SECOND(6, 6)");
        assertRoundTrip("INTERVAL SECOND(13, 3)");
        assertRoundTrip("INTERVAL DAY(2) TO SECOND(3)");
        assertRoundTrip("INTERVAL HOUR(4) TO SECOND(0)");
    }

    @Test
    public void testIntervalPrecisionVariable()
    {
        // an interval type in a function signature can carry a precision variable, like timestamp(p);
        // the trailing SECOND takes the implicit fractional-seconds precision of 6
        TypeTemplate template = parseTypeTemplate("interval day(p) to second", Set.of(), Set.of("p"));
        assertThat(TypeSyntax.toSql(TypeTemplates.bind(template, Map.of(), Map.of("p", 4L)))).isEqualTo("interval day(4) to second(6)");
        assertThat(TypeSyntax.toSql(TypeTemplates.bind(template, Map.of(), Map.of("p", 9L)))).isEqualTo("interval day(9) to second(6)");
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
    public void testParseTypeDescriptorPreservesRowFieldNameCase()
    {
        // Regression test for a cache poisoning bug where DATA_TYPE_CACHE was keyed by the lowercased signature
        // while storing the DataType parsed from the original-cased signature. The first case-variant of a row
        // signature would then win for every later lookup, returning field names with the wrong casing.
        // Downstream, NamedTypeSignature.equals is case-sensitive on field names, so function dependency
        // resolution would miss and throw UndeclaredDependencyException at filter compile time.
        TypeDescriptor camelCaseFirst = parseTypeDescriptor("row(\"memberId\" integer, \"viewerUrn\" varchar)");
        TypeDescriptor lowerCaseAfter = parseTypeDescriptor("row(\"memberid\" integer, \"viewerurn\" varchar)");

        assertThat(camelCaseFirst.toString())
                .isEqualTo("row(\"memberId\" integer,\"viewerUrn\" varchar(2147483647))");
        assertThat(lowerCaseAfter.toString())
                .isEqualTo("row(\"memberid\" integer,\"viewerurn\" varchar(2147483647))");
        assertThat(camelCaseFirst).isNotEqualTo(lowerCaseAfter);

        // Reverse order: lowercase first, camelCase second. Both must still preserve their input casing.
        TypeDescriptor lowerCaseFirst = parseTypeDescriptor("row(\"actorurn\" varchar, \"appname\" varchar)");
        TypeDescriptor camelCaseAfter = parseTypeDescriptor("row(\"actorUrn\" varchar, \"appName\" varchar)");

        assertThat(lowerCaseFirst.toString())
                .isEqualTo("row(\"actorurn\" varchar(2147483647),\"appname\" varchar(2147483647))");
        assertThat(camelCaseAfter.toString())
                .isEqualTo("row(\"actorUrn\" varchar(2147483647),\"appName\" varchar(2147483647))");
        assertThat(lowerCaseFirst).isNotEqualTo(camelCaseAfter);
    }
}
