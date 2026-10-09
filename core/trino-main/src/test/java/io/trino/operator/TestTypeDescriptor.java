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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSyntax;
import io.trino.spi.type.VarcharType;
import io.trino.sql.parser.ParsingException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeDescriptor
{
    @Test
    public void parseRowDescriptor()
    {
        // row descriptor with named fields
        assertRowDescriptor(
                "row(a bigint,b varchar)",
                rowDescriptor(namedParameter("a", descriptor("bigint")), namedParameter("b", varchar())));
        assertRowDescriptor(
                "row(a bigint,b array(bigint),c row(a bigint))",
                rowDescriptor(
                        namedParameter("a", descriptor("bigint")),
                        namedParameter("b", array(descriptor("bigint"))),
                        namedParameter("c", rowDescriptor(namedParameter("a", descriptor("bigint"))))));
        assertRowDescriptor(
                "row(a varchar(10),b row(a bigint))",
                rowDescriptor(
                        namedParameter("a", varchar(10)),
                        namedParameter("b", rowDescriptor(namedParameter("a", descriptor("bigint"))))));
        assertRowDescriptor(
                "array(row(col0 bigint,col1 double))",
                array(rowDescriptor(namedParameter("col0", descriptor("bigint")), namedParameter("col1", descriptor("double")))));
        assertRowDescriptor(
                "row(col0 array(row(col0 bigint,col1 double)))",
                rowDescriptor(namedParameter("col0", array(
                        rowDescriptor(namedParameter("col0", descriptor("bigint")), namedParameter("col1", descriptor("double")))))));

        // row with mixed fields
        assertRowDescriptor(
                "row(bigint,varchar)",
                rowDescriptor(unnamedParameter(descriptor("bigint")), unnamedParameter(varchar())));
        assertRowDescriptor(
                "row(bigint,array(bigint),row(a bigint))",
                rowDescriptor(
                        unnamedParameter(descriptor("bigint")),
                        unnamedParameter(array(descriptor("bigint"))),
                        unnamedParameter(rowDescriptor(namedParameter("a", descriptor("bigint"))))));
        assertRowDescriptor(
                "row(varchar(10),b row(bigint))",
                rowDescriptor(
                        unnamedParameter(varchar(10)),
                        namedParameter("b", rowDescriptor(unnamedParameter(descriptor("bigint"))))));
        assertRowDescriptor(
                "array(row(col0 bigint,double))",
                array(rowDescriptor(namedParameter("col0", descriptor("bigint")), unnamedParameter(descriptor("double")))));
        assertRowDescriptor(
                "row(col0 array(row(bigint,double)))",
                rowDescriptor(namedParameter("col0", array(
                        rowDescriptor(unnamedParameter(descriptor("bigint")), unnamedParameter(descriptor("double")))))));

        // named fields of types with spaces
        assertRowDescriptor(
                "row(time time with time zone)",
                rowDescriptor(namedParameter("time", descriptor(StandardTypes.TIME_WITH_TIME_ZONE))));
        assertRowDescriptor(
                "row(time timestamp with time zone)",
                rowDescriptor(namedParameter("time", descriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE))));
        assertRowDescriptor(
                "row(interval interval day to second)",
                rowDescriptor(namedParameter("interval", interval(StandardTypes.INTERVAL_DAY_TO_SECOND, 2, 5))));
        assertRowDescriptor(
                "row(interval interval year to month)",
                rowDescriptor(namedParameter("interval", interval(StandardTypes.INTERVAL_YEAR_TO_MONTH, 0, 1))));
        assertRowDescriptor(
                "row(double double precision)",
                rowDescriptor(namedParameter("double", descriptor("double"))));

        // unnamed fields of types with spaces
        assertRowDescriptor(
                "row(time with time zone)",
                rowDescriptor(unnamedParameter(descriptor(StandardTypes.TIME_WITH_TIME_ZONE))));
        assertRowDescriptor(
                "row(timestamp with time zone)",
                rowDescriptor(unnamedParameter(descriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE))));
        assertRowDescriptor(
                "row(interval day to second)",
                rowDescriptor(unnamedParameter(interval(StandardTypes.INTERVAL_DAY_TO_SECOND, 2, 5))));
        assertRowDescriptor(
                "row(interval year to month)",
                rowDescriptor(unnamedParameter(interval(StandardTypes.INTERVAL_YEAR_TO_MONTH, 0, 1))));
        assertRowDescriptor(
                "row(double precision)",
                rowDescriptor(unnamedParameter(descriptor("double"))));
        assertRowDescriptor(
                "row(array(time with time zone))",
                rowDescriptor(unnamedParameter(array(descriptor(StandardTypes.TIME_WITH_TIME_ZONE)))));
        assertRowDescriptor(
                "row(map(timestamp with time zone,interval day to second))",
                rowDescriptor(unnamedParameter(map(descriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE), interval(StandardTypes.INTERVAL_DAY_TO_SECOND, 2, 5)))));

        // quoted field names
        assertRowDescriptor(
                "row(\"time with time zone\" time with time zone,\"double\" double)",
                rowDescriptor(
                        namedParameter("time with time zone", descriptor(StandardTypes.TIME_WITH_TIME_ZONE)),
                        namedParameter("double", descriptor("double"))));

        // allow spaces
        assertDescriptor(
                "row( time  time with time zone, array( interval day to second ) )",
                "row",
                ImmutableList.of("\"time\" time with time zone", "array(interval day(2) to second(6))"),
                "row(\"time\" time with time zone,array(interval day(2) to second(6)))");

        // preserve base name case
        assertRowDescriptor(
                "RoW(a bigint,b varchar)",
                rowDescriptor(namedParameter("a", descriptor("bigint")), namedParameter("b", varchar())));
    }

    private TypeDescriptor varchar()
    {
        return new TypeDescriptor(StandardTypes.VARCHAR, TypeParameter.numericParameter(VarcharType.UNBOUNDED_LENGTH));
    }

    private TypeDescriptor varchar(long length)
    {
        return new TypeDescriptor(StandardTypes.VARCHAR, TypeParameter.numericParameter(length));
    }

    private static TypeDescriptor rowDescriptor(Field... fields)
    {
        return new TypeDescriptor(
                "row",
                asList(fields).stream()
                        .map(field -> TypeParameter.typeParameter(field.name(), field.type()))
                        .collect(toImmutableList()));
    }

    private static Field namedParameter(String name, TypeDescriptor value)
    {
        return new Field(Optional.of(name), value);
    }

    private static Field unnamedParameter(TypeDescriptor value)
    {
        return new Field(Optional.empty(), value);
    }

    private static TypeDescriptor array(TypeDescriptor type)
    {
        return new TypeDescriptor(StandardTypes.ARRAY, TypeParameter.typeParameter(type));
    }

    private static TypeDescriptor map(TypeDescriptor keyType, TypeDescriptor valueType)
    {
        return new TypeDescriptor(StandardTypes.MAP, TypeParameter.typeParameter(keyType), TypeParameter.typeParameter(valueType));
    }

    private TypeDescriptor descriptor(String name)
    {
        return new TypeDescriptor(name);
    }

    // An interval descriptor is parametric: the parser fills a bare qualifier with the start and end
    // field codes (year=0 .. second=5) and the implicit leading precision of 2. A day-time interval
    // also carries the implicit fractional-seconds precision of 6 in a fourth parameter.
    private static TypeDescriptor interval(String base, long startField, long endField)
    {
        if (base.equals(StandardTypes.INTERVAL_YEAR_TO_MONTH)) {
            return new TypeDescriptor(base, TypeParameter.numericParameter(startField), TypeParameter.numericParameter(endField), TypeParameter.numericParameter(2));
        }
        return new TypeDescriptor(base, TypeParameter.numericParameter(startField), TypeParameter.numericParameter(endField), TypeParameter.numericParameter(2), TypeParameter.numericParameter(6));
    }

    @Test
    public void parseDescriptor()
    {
        assertDescriptor("boolean", "boolean", ImmutableList.of());
        // parsing the SQL `varchar` yields the unbounded descriptor, which renders back to the bare `varchar`
        // surface even though the parameter carries the sentinel length
        assertDescriptor("varchar", "varchar", ImmutableList.of(Integer.toString(VarcharType.UNBOUNDED_LENGTH)), "varchar");

        assertDescriptor("array(bigint)", "array", ImmutableList.of("bigint"));

        assertDescriptor("array(array(bigint))", "array", ImmutableList.of("array(bigint)"));
        assertDescriptor(
                "array(timestamp with time zone)",
                "array",
                ImmutableList.of("timestamp with time zone"));

        assertDescriptor(
                "map(bigint,bigint)",
                "map",
                ImmutableList.of("bigint", "bigint"));
        assertDescriptor(
                "map(bigint,array(bigint))",
                "map",
                ImmutableList.of("bigint", "array(bigint)"));
        // a nested unbounded varchar renders back to the bare `varchar` surface
        assertDescriptor(
                "map(bigint,map(bigint,map(varchar,bigint)))",
                "map",
                ImmutableList.of("bigint", "map(bigint,map(varchar,bigint))"));

        assertDescriptorFail("blah()");
        assertDescriptorFail("array()");
        assertDescriptorFail("map()");

        // ensure this is not treated as a row type
        assertDescriptor("rowxxx(a)", "rowxxx", ImmutableList.of("a"));
    }

    @Test
    public void parseWithLiteralParameters()
    {
        assertDescriptor("foo(42)", "foo", ImmutableList.of("42"));
        assertDescriptor("varchar(10)", "varchar", ImmutableList.of("10"));
    }

    @Test
    public void testInternalFormRoundTrip()
    {
        // TypeDescriptor.fromString parses the internal base(arg, …) IR (toString/jsonValue) back to an
        // equal descriptor — the (de)serialization round-trip, including the special types whose IR
        // diverges from their SQL spelling, and nested/named cases.
        List<TypeDescriptor> descriptors = ImmutableList.of(
                new TypeDescriptor(StandardTypes.INTERVAL_DAY_TO_SECOND),
                new TypeDescriptor(StandardTypes.INTERVAL_YEAR_TO_MONTH),
                new TypeDescriptor("bigint"),
                new TypeDescriptor("varchar", TypeParameter.numericParameter(VarcharType.UNBOUNDED_LENGTH)),
                new TypeDescriptor("decimal", TypeParameter.numericParameter(10), TypeParameter.numericParameter(2)),
                new TypeDescriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeParameter.numericParameter(6)),
                new TypeDescriptor(StandardTypes.TIME_WITH_TIME_ZONE, TypeParameter.numericParameter(9)),
                new TypeDescriptor("array", TypeParameter.typeParameter(new TypeDescriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeParameter.numericParameter(3)))),
                new TypeDescriptor("map", TypeParameter.typeParameter(new TypeDescriptor("varchar", TypeParameter.numericParameter(VarcharType.UNBOUNDED_LENGTH))), TypeParameter.typeParameter(new TypeDescriptor("bigint"))),
                new TypeDescriptor("row", TypeParameter.namedField("a", new TypeDescriptor("bigint")), TypeParameter.namedField("a\"b,c", new TypeDescriptor("varchar", TypeParameter.numericParameter(10)))));
        for (TypeDescriptor descriptor : descriptors) {
            assertThat(TypeDescriptor.fromString(descriptor.toString())).isEqualTo(descriptor);
            assertThat(TypeDescriptor.fromString(descriptor.jsonValue())).isEqualTo(descriptor);
        }

        // Corruption in a machine-generated id surfaces as an exception, not a silently wrong descriptor.
        assertThatThrownBy(() -> TypeDescriptor.fromString("array(bigint")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TypeDescriptor.fromString("varchar()")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testVarchar()
    {
        // toString() is the internal IR and keeps the sentinel length; getTypeId() is the serialized
        // SQL spelling, which elides it.
        assertThat(VARCHAR.getTypeDescriptor().toString()).isEqualTo("varchar(2147483647)");
        assertThat(VARCHAR.getTypeId().getId()).isEqualTo("varchar");
        assertThat(createVarcharType(42).getTypeDescriptor().toString()).isEqualTo("varchar(42)");
        assertThat(VARCHAR.getTypeDescriptor()).isEqualTo(createUnboundedVarcharType().getTypeDescriptor());
        assertThat(createUnboundedVarcharType().getTypeDescriptor()).isEqualTo(VARCHAR.getTypeDescriptor());
        assertThat(VARCHAR.getTypeDescriptor().hashCode()).isEqualTo(createUnboundedVarcharType().getTypeDescriptor().hashCode());
        assertThat(createUnboundedVarcharType().getTypeDescriptor())
                .isNotEqualTo(createVarcharType(10).getTypeDescriptor());
    }

    @Test
    public void testMalformedInternalDescriptor()
    {
        for (String value : List.of("", "bigint)", "bigint)(bigint)", "row(\"a\" bigint foo)", "row(\"a bigint)", "array(bigint))", "array(bigint)(bigint)", "array(,bigint)", "array(bigint,)", "123", "-123")) {
            assertThatThrownBy(() -> TypeDescriptor.fromString(value))
                    .as("descriptor: %s", value)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (String base : List.of("123", "-123", "bigint)", "array(bigint)", "\"bigint\"", "bigint foo")) {
            assertThatThrownBy(() -> new TypeDescriptor(base))
                    .as("base: %s", base)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void assertRowDescriptor(
            String typeName,
            TypeDescriptor expectedDescriptor)
    {
        TypeDescriptor descriptor = parseTypeDescriptor(typeName);
        assertThat(descriptor).isEqualTo(expectedDescriptor);
    }

    private static void assertDescriptor(String typeName, String base, List<String> parameters)
    {
        assertDescriptor(typeName, base, parameters, typeName);
    }

    private static void assertDescriptor(
            String typeName,
            String base,
            List<String> parameters,
            String expectedTypeName)
    {
        TypeDescriptor descriptor = parseTypeDescriptor(typeName);
        assertThat(descriptor.getBase()).isEqualTo(base);
        assertThat(descriptor.getParameters()).hasSize(parameters.size());
        for (int i = 0; i < descriptor.getParameters().size(); i++) {
            assertThat(TypeSyntax.toSql(descriptor.getParameters().get(i))).isEqualTo(parameters.get(i));
        }
        assertThat(TypeSyntax.toSql(descriptor)).isEqualTo(expectedTypeName);
    }

    private void assertDescriptorFail(String typeName)
    {
        assertThatThrownBy(() -> parseTypeDescriptor(typeName))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line [1-9][0-9]*:[1-9][0-9]*: mismatched input '.*'\\. Expecting: .*");
    }

    record Field(Optional<String> name, TypeDescriptor type) {}
}
