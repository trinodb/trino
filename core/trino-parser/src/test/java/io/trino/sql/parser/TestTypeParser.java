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
package io.trino.sql.parser;

import org.junit.jupiter.api.Test;

import static io.trino.sql.parser.ParserAssert.type;
import static io.trino.sql.parser.TreeNodes.dateTimeType;
import static io.trino.sql.parser.TreeNodes.field;
import static io.trino.sql.parser.TreeNodes.identifier;
import static io.trino.sql.parser.TreeNodes.intervalType;
import static io.trino.sql.parser.TreeNodes.location;
import static io.trino.sql.parser.TreeNodes.parameter;
import static io.trino.sql.parser.TreeNodes.parametricType;
import static io.trino.sql.parser.TreeNodes.rowType;
import static io.trino.sql.parser.TreeNodes.simpleType;
import static io.trino.sql.tree.DateTimeDataType.Type.TIME;
import static io.trino.sql.tree.DateTimeDataType.Type.TIMESTAMP;
import static io.trino.sql.tree.IntervalDayTimeDataType.Field.DAY;
import static io.trino.sql.tree.IntervalDayTimeDataType.Field.MONTH;
import static io.trino.sql.tree.IntervalDayTimeDataType.Field.SECOND;
import static io.trino.sql.tree.IntervalDayTimeDataType.Field.YEAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTypeParser
{
    @Test
    public void testCaseVariants()
    {
        assertThat(type("varchar"))
                .isEqualTo(simpleType(location(1, 1), "varchar"));

        assertThat(type("VARCHAR"))
                .isEqualTo(simpleType(location(1, 1), "VARCHAR"));

        assertThat(type("Varchar"))
                .isEqualTo(simpleType(location(1, 1), "Varchar"));

        assertThat(type("ARRAY(bigint)"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "ARRAY"),
                        parameter(simpleType(location(1, 7), "bigint"))));

        assertThat(type("Array(Bigint)"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "Array"),
                        parameter(simpleType(location(1, 7), "Bigint"))));

        assertThat(type("array(bigint)"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "array"),
                        parameter(simpleType(location(1, 7), "bigint"))));
    }

    @Test
    public void testDoublePrecisionVariants()
    {
        assertThat(type("DOUBLE PRECISION"))
                .isEqualTo(simpleType(location(1, 1), "DOUBLE"));

        assertThat(type("DOUBLE     PRECISION"))
                .isEqualTo(simpleType(location(1, 1), "DOUBLE"));

        assertThat(type("double precision"))
                .isEqualTo(simpleType(location(1, 1), "double"));

        assertThat(type("ROW(DOUBLE PRECISION)"))
                .isEqualTo(rowType(
                        location(1, 1),
                        field(location(1, 5), simpleType(location(1, 5), "DOUBLE"))));
    }

    @Test
    public void testSimpleTypes()
    {
        assertThat(type("VARCHAR"))
                .isEqualTo(simpleType(location(1, 1), "VARCHAR"));

        assertThat(type("BIGINT"))
                .isEqualTo(simpleType(location(1, 1), "BIGINT"));

        assertThat(type("DOUBLE"))
                .isEqualTo(simpleType(location(1, 1), "DOUBLE"));

        assertThat(type("BOOLEAN"))
                .isEqualTo(simpleType(location(1, 1), "BOOLEAN"));
    }

    @Test
    public void testDayTimeTypes()
    {
        assertThat(type("TIMESTAMP"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false));

        assertThat(type("TIMESTAMP WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false));

        assertThat(type("TIMESTAMP WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, true));

        assertThat(type("TIMESTAMP(3)"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false, parameter(location(1, 11), "3")));

        assertThat(type("TIMESTAMP(3) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false, parameter(location(1, 11), "3")));

        assertThat(type("TIMESTAMP(3) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, true, parameter(location(1, 11), "3")));

        assertThat(type("TIMESTAMP(p)"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false, parameter(simpleType(location(1, 11), "p"))));

        assertThat(type("TIMESTAMP(p) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, false, parameter(simpleType(location(1, 11), "p"))));

        assertThat(type("TIMESTAMP(p) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIMESTAMP, true, parameter(simpleType(location(1, 11), "p"))));

        assertThat(type("TIME"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false));

        assertThat(type("TIME WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false));

        assertThat(type("TIME WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, true));

        assertThat(type("TIME(3)"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false, parameter(location(1, 6), "3")));

        assertThat(type("TIME(3) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false, parameter(location(1, 6), "3")));

        assertThat(type("TIME(3) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, true, parameter(location(1, 6), "3")));

        assertThat(type("TIME(p)"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false, parameter(simpleType(location(1, 6), "p"))));

        assertThat(type("TIME(p) WITHOUT TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, false, parameter(simpleType(location(1, 6), "p"))));

        assertThat(type("TIME(p) WITH TIME ZONE"))
                .isEqualTo(dateTimeType(location(1, 1), TIME, true, parameter(simpleType(location(1, 6), "p"))));
    }

    @Test
    public void testIntervalTypes()
    {
        assertThat(type("INTERVAL YEAR TO DAY"))
                .isEqualTo(intervalType(location(1, 1), YEAR, DAY));

        assertThat(type("INTERVAL YEAR TO MONTH"))
                .isEqualTo(intervalType(location(1, 1), YEAR, MONTH));

        assertThat(type("INTERVAL SECOND"))
                .isEqualTo(intervalType(location(1, 1), SECOND, SECOND));
    }

    @Test
    public void testParametricTypes()
    {
        assertThat(type("ARRAY(TINYINT)"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "ARRAY"),
                        parameter(simpleType(location(1, 7), "TINYINT"))));

        assertThat(type("ARRAY ( TINYINT ) "))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "ARRAY"),
                        parameter(simpleType(location(1, 9), "TINYINT"))));

        assertThat(type("MAP(BIGINT, SMALLINT)"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        "MAP",
                        parameter(simpleType(location(1, 5), "BIGINT")),
                        parameter(simpleType(location(1, 13), "SMALLINT"))));
    }

    @Test
    public void testArray()
    {
        assertThat(type("foo(42, 55) ARRAY"))
                .isEqualTo(parametricType(location(1, 1),
                        identifier(location(1, 13), "ARRAY"),
                        parameter(parametricType(location(1, 1), "foo",
                                parameter(location(1, 5), "42"),
                                parameter(location(1, 9), "55")))));

        assertThat(type("VARCHAR(7) ARRAY"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 12), "ARRAY"),
                        parameter(parametricType(
                                location(1, 1),
                                identifier(location(1, 1), "VARCHAR"),
                                parameter(location(1, 9), "7")))));

        assertThat(type("VARCHAR(7) ARRAY array"))
                .isEqualTo(
                        parametricType(
                                location(1, 1),
                                identifier(location(1, 18), "array"),
                                parameter(
                                        parametricType(
                                                location(1, 1),
                                                identifier(location(1, 12), "ARRAY"),
                                                parameter(parametricType(
                                                        location(1, 1),
                                                        identifier(location(1, 1), "VARCHAR"),
                                                        parameter(location(1, 9), "7")))))));
    }

    @Test
    public void testRowType()
    {
        assertThat(type("ROW(a BIGINT, b VARCHAR)"))
                .isEqualTo(rowType(
                        location(1, 1),
                        field(location(1, 5), "a", simpleType(location(1, 7), "BIGINT")),
                        field(location(1, 15), "b", simpleType(location(1, 17), "VARCHAR"))));

        assertThat(type("ROW(a BIGINT,b VARCHAR)"))
                .describedAs("No space after comma")
                .isEqualTo(rowType(
                        location(1, 1),
                        field(location(1, 5), "a", simpleType(location(1, 7), "BIGINT")),
                        field(location(1, 14), "b", simpleType(location(1, 16), "VARCHAR"))));

        assertThat(type("ROW(\"a\" BIGINT, \"b\" VARCHAR)"))
                .isEqualTo(rowType(
                        location(1, 1),
                        field(location(1, 5), "a", true, simpleType(location(1, 9), "BIGINT")),
                        field(location(1, 17), "b", true, simpleType(location(1, 21), "VARCHAR"))));
    }

    @Test
    public void testComplexTypes()
    {
        assertThat(type("ROW(x BIGINT, y DOUBLE PRECISION, z ROW(m array<bigint>,n map<double,varchar>))"))
                .isEqualTo(rowType(
                        location(1, 1),
                        field(location(1, 5), "x", simpleType(location(1, 7), "BIGINT")),
                        field(location(1, 15), "y", simpleType(location(1, 17), "DOUBLE")),
                        field(location(1, 35), "z", rowType(
                                location(1, 37),
                                field(location(1, 41), "m", parametricType(
                                        location(1, 43),
                                        "array",
                                        parameter(simpleType(location(1, 49), "bigint")))),
                                field(location(1, 57), "n", parametricType(
                                        location(1, 59),
                                        "map",
                                        parameter(simpleType(location(1, 63), "double")),
                                        parameter(simpleType(location(1, 70), "varchar"))))))));
    }

    @Test
    public void testLegacyTypes()
    {
        assertThat(type("ARRAY<BIGINT>"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "ARRAY"),
                        parameter(simpleType(location(1, 7), "BIGINT"))));

        assertThat(type("ARRAY < BIGINT > "))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 1), "ARRAY"),
                        parameter(simpleType(location(1, 9), "BIGINT"))));

        assertThat(type("ARRAY<ARRAY<BIGINT>>"))
                .isEqualTo(parametricType(location(1, 1), "ARRAY", parameter(
                        parametricType(
                                location(1, 7),
                                "ARRAY",
                                parameter(simpleType(location(1, 13), "BIGINT"))))));

        assertThat(type("ARRAY<array<varchar(42)>>"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        "ARRAY",
                        parameter(parametricType(
                                location(1, 7),
                                "array",
                                parameter(parametricType(
                                        location(1, 13),
                                        "varchar",
                                        parameter(location(1, 21), "42")))))));

        assertThat(type("ARRAY<varchar(42)>"))
                .isEqualTo(parametricType(location(1, 1), "ARRAY", parameter(
                        parametricType(
                                location(1, 7),
                                "varchar",
                                parameter(location(1, 15), "42")))));

        assertThat(type("MAP<BIGINT, VARCHAR>"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        "MAP",
                        parameter(simpleType(location(1, 5), "BIGINT")),
                        parameter(simpleType(location(1, 13), "VARCHAR"))));

        assertThat(type("MAP<BIGINT, VARCHAR> ARRAY"))
                .isEqualTo(parametricType(
                        location(1, 1),
                        identifier(location(1, 22), "ARRAY"),
                        parameter(
                                parametricType(
                                        location(1, 1),
                                        "MAP",
                                        parameter(simpleType(location(1, 5), "BIGINT")),
                                        parameter(simpleType(location(1, 13), "VARCHAR"))))));
    }
}
