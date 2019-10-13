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
package io.prestosql.sql.parser;

import org.testng.annotations.Test;

import static io.prestosql.sql.parser.ParserAssert.type;
import static io.prestosql.sql.parser.TreeNodes.dateTimeType;
import static io.prestosql.sql.parser.TreeNodes.field;
import static io.prestosql.sql.parser.TreeNodes.identifier;
import static io.prestosql.sql.parser.TreeNodes.intervalType;
import static io.prestosql.sql.parser.TreeNodes.location;
import static io.prestosql.sql.parser.TreeNodes.parameter;
import static io.prestosql.sql.parser.TreeNodes.parametricType;
import static io.prestosql.sql.parser.TreeNodes.rowType;
import static io.prestosql.sql.parser.TreeNodes.simpleType;
import static io.prestosql.sql.tree.DateTimeDataType.Type.TIME;
import static io.prestosql.sql.tree.DateTimeDataType.Type.TIMESTAMP;
import static io.prestosql.sql.tree.IntervalDayTimeDataType.Field.DAY;
import static io.prestosql.sql.tree.IntervalDayTimeDataType.Field.MONTH;
import static io.prestosql.sql.tree.IntervalDayTimeDataType.Field.SECOND;
import static io.prestosql.sql.tree.IntervalDayTimeDataType.Field.YEAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeParser
{
    @Test
    public void testCaseVariants()
    {
        assertThat(type("varchar"))
                .isEqualTo(simpleType(location(1, 0), "varchar"));

        assertThat(type("VARCHAR"))
                .isEqualTo(simpleType(location(1, 0), "VARCHAR"));

        assertThat(type("Varchar"))
                .isEqualTo(simpleType(location(1, 0), "Varchar"));

        assertThat(type("ARRAY(bigint)"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "ARRAY"),
                        parameter(simpleType(location(1, 6), "bigint"))));

        assertThat(type("Array(Bigint)"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "Array"),
                        parameter(simpleType(location(1, 6), "Bigint"))));

        assertThat(type("array(bigint)"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "array"),
                        parameter(simpleType(location(1, 6), "bigint"))));
    }

    @Test
    public void testDoublePrecisionVariants()
    {
        assertThat(type("DOUBLE PRECISION"))
                .isEqualTo(simpleType(location(1, 0), "DOUBLE"));

        assertThat(type("DOUBLE     PRECISION"))
                .isEqualTo(simpleType(location(1, 0), "DOUBLE"));

        assertThat(type("double precision"))
                .isEqualTo(simpleType(location(1, 0), "double"));
    }

    @Test
    public void testSimpleTypes()
    {
        assertThat(type("VARCHAR"))
                .isEqualTo(simpleType(location(1, 0), "VARCHAR"));

        assertThat(type("BIGINT"))
                .isEqualTo(simpleType(location(1, 0), "BIGINT"));

        assertThat(type("DOUBLE"))
                .isEqualTo(simpleType(location(1, 0), "DOUBLE"));

        assertThat(type("BOOLEAN"))
                .isEqualTo(simpleType(location(1, 0), "BOOLEAN"));
    }

    @Test
    public void testDayTimeTypes()
    {
        assertThat(type("TIMESTAMP"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, false));

        assertThat(type("TIMESTAMP WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, false));

        assertThat(type("TIMESTAMP WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, true));

        assertThat(type("TIMESTAMP(3)"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, false, "3"));

        assertThat(type("TIMESTAMP(3) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, false, "3"));

        assertThat(type("TIMESTAMP(3) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIMESTAMP, true, "3"));

        assertThat(type("TIME"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, false));

        assertThat(type("TIME WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, false));

        assertThat(type("TIME WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, true));

        assertThat(type("TIME(3)"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, false, "3"));

        assertThat(type("TIME(3) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, false, "3"));

        assertThat(type("TIME(3) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 0), TIME, true, "3"));
    }

    @Test
    public void testIntervalTypes()
    {
        assertThat(type("INTERVAL YEAR TO DAY"))
                .isEqualTo(intervalType(location(1, 0), YEAR, DAY));

        assertThat(type("INTERVAL YEAR TO MONTH"))
                .isEqualTo(intervalType(location(1, 0), YEAR, MONTH));

        assertThat(type("INTERVAL SECOND"))
                .isEqualTo(intervalType(location(1, 0), SECOND, SECOND));
    }

    @Test
    public void testParametricTypes()
    {
        assertThat(type("ARRAY(TINYINT)"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "ARRAY"),
                        parameter(simpleType(location(1, 6), "TINYINT"))));

        assertThat(type("ARRAY ( TINYINT ) "))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "ARRAY"),
                        parameter(simpleType(location(1, 8), "TINYINT"))));

        assertThat(type("MAP(BIGINT, SMALLINT)"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        "MAP",
                        parameter(simpleType(location(1, 4), "BIGINT")),
                        parameter(simpleType(location(1, 12), "SMALLINT"))));
    }

    @Test
    public void testArray()
    {
        assertThat(type("foo(42, 55) ARRAY"))
                .isEqualTo(parametricType(location(1, 0),
                        identifier(location(1, 12), "ARRAY"),
                        parameter(parametricType(location(1, 0), "foo",
                                parameter(location(1, 4), "42"),
                                parameter(location(1, 8), "55")))));

        assertThat(type("VARCHAR(7) ARRAY"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 11), "ARRAY"),
                        parameter(parametricType(
                                location(1, 0),
                                identifier(location(1, 0), "VARCHAR"),
                                parameter(location(1, 8), "7")))));

        assertThat(type("VARCHAR(7) ARRAY array"))
                .isEqualTo(
                        parametricType(
                                location(1, 0),
                                identifier(location(1, 17), "array"),
                                parameter(
                                        parametricType(
                                                location(1, 0),
                                                identifier(location(1, 11), "ARRAY"),
                                                parameter(parametricType(
                                                        location(1, 0),
                                                        identifier(location(1, 0), "VARCHAR"),
                                                        parameter(location(1, 8), "7")))))));
    }

    @Test
    public void testRowType()
    {
        assertThat(type("ROW(a BIGINT, b VARCHAR)"))
                .isEqualTo(rowType(
                        location(1, 0),
                        field(location(1, 4), "a", simpleType(location(1, 6), "BIGINT")),
                        field(location(1, 14), "b", simpleType(location(1, 16), "VARCHAR"))));

        assertThat(type("ROW(a BIGINT,b VARCHAR)"))
                .describedAs("No space after comma")
                .isEqualTo(rowType(
                        location(1, 0),
                        field(location(1, 4), "a", simpleType(location(1, 6), "BIGINT")),
                        field(location(1, 13), "b", simpleType(location(1, 15), "VARCHAR"))));

        assertThat(type("ROW(\"a\" BIGINT, \"b\" VARCHAR)"))
                .isEqualTo(rowType(
                        location(1, 0),
                        field(location(1, 4), "a", true, simpleType(location(1, 8), "BIGINT")),
                        field(location(1, 16), "b", true, simpleType(location(1, 20), "VARCHAR"))));
    }

    @Test
    public void testComplexTypes()
    {
        assertThat(type("ROW(x BIGINT, y DOUBLE PRECISION, z ROW(m array<bigint>,n map<double,varchar>))"))
                .isEqualTo(rowType(
                        location(1, 0),
                        field(location(1, 4), "x", simpleType(location(1, 6), "BIGINT")),
                        field(location(1, 14), "y", simpleType(location(1, 16), "DOUBLE")),
                        field(location(1, 34), "z", rowType(
                                location(1, 36),
                                field(location(1, 40), "m", parametricType(
                                        location(1, 42),
                                        "array",
                                        parameter(simpleType(location(1, 48), "bigint")))),
                                field(location(1, 56), "n", parametricType(
                                        location(1, 58),
                                        "map",
                                        parameter(simpleType(location(1, 62), "double")),
                                        parameter(simpleType(location(1, 69), "varchar"))))))));
    }

    @Test
    public void testLegacyTypes()
    {
        assertThat(type("ARRAY<BIGINT>"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "ARRAY"),
                        parameter(simpleType(location(1, 6), "BIGINT"))));

        assertThat(type("ARRAY < BIGINT > "))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 0), "ARRAY"),
                        parameter(simpleType(location(1, 8), "BIGINT"))));

        assertThat(type("ARRAY<ARRAY<BIGINT>>"))
                .isEqualTo(parametricType(location(1, 0), "ARRAY", parameter(
                        parametricType(
                                location(1, 6),
                                "ARRAY",
                                parameter(simpleType(location(1, 12), "BIGINT"))))));

        assertThat(type("ARRAY<array<varchar(42)>>"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        "ARRAY",
                        parameter(parametricType(
                                location(1, 6),
                                "array",
                                parameter(parametricType(
                                        location(1, 12),
                                        "varchar",
                                        parameter(location(1, 20), "42")))))));

        assertThat(type("ARRAY<varchar(42)>"))
                .isEqualTo(parametricType(location(1, 0), "ARRAY", parameter(
                        parametricType(
                                location(1, 6),
                                "varchar",
                                parameter(location(1, 14), "42")))));

        assertThat(type("MAP<BIGINT, VARCHAR>"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        "MAP",
                        parameter(simpleType(location(1, 4), "BIGINT")),
                        parameter(simpleType(location(1, 12), "VARCHAR"))));

        assertThat(type("MAP<BIGINT, VARCHAR> ARRAY"))
                .isEqualTo(parametricType(
                        location(1, 0),
                        identifier(location(1, 21), "ARRAY"),
                        parameter(
                                parametricType(
                                        location(1, 0),
                                        "MAP",
                                        parameter(simpleType(location(1, 4), "BIGINT")),
                                        parameter(simpleType(location(1, 12), "VARCHAR"))))));
    }
}
