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
import io.trino.spi.type.VarcharType;
import io.trino.sql.parser.ParsingException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TypeParameter.numericParameter;
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
    public void parseRowSignature()
    {
        // row signature with named fields
        assertRowSignature(
                "row(a bigint,b varchar)",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));
        assertRowSignature(
                "row(a bigint,b array(bigint),c row(a bigint))",
                rowSignature(
                        namedParameter("a", signature("bigint")),
                        namedParameter("b", array(signature("bigint"))),
                        namedParameter("c", rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "row(a varchar(10),b row(a bigint))",
                rowSignature(
                        namedParameter("a", varchar(10)),
                        namedParameter("b", rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,col1 double))",
                array(rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))));
        assertRowSignature(
                "row(col0 array(row(col0 bigint,col1 double)))",
                rowSignature(namedParameter("col0", array(
                        rowSignature(namedParameter("col0", signature("bigint")), namedParameter("col1", signature("double")))))));

        // row with mixed fields
        assertRowSignature(
                "row(bigint,varchar)",
                rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(varchar())));
        assertRowSignature(
                "row(bigint,array(bigint),row(a bigint))",
                rowSignature(
                        unnamedParameter(signature("bigint")),
                        unnamedParameter(array(signature("bigint"))),
                        unnamedParameter(rowSignature(namedParameter("a", signature("bigint"))))));
        assertRowSignature(
                "row(varchar(10),b row(bigint))",
                rowSignature(
                        unnamedParameter(varchar(10)),
                        namedParameter("b", rowSignature(unnamedParameter(signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,double))",
                array(rowSignature(namedParameter("col0", signature("bigint")), unnamedParameter(signature("double")))));
        assertRowSignature(
                "row(col0 array(row(bigint,double)))",
                rowSignature(namedParameter("col0", array(
                        rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(signature("double")))))));

        // named fields of types with spaces
        assertRowSignature(
                "row(time time with time zone)",
                rowSignature(namedParameter("time", signature("time with time zone"))));
        assertRowSignature(
                "row(time timestamp with time zone)",
                rowSignature(namedParameter("time", signature("timestamp with time zone"))));
        assertRowSignature(
                "row(interval interval day to second)",
                rowSignature(namedParameter("interval", signature("interval day to second"))));
        assertRowSignature(
                "row(interval interval year to month)",
                rowSignature(namedParameter("interval", signature("interval year to month"))));
        assertRowSignature(
                "row(double double precision)",
                rowSignature(namedParameter("double", signature("double"))));

        // unnamed fields of types with spaces
        assertRowSignature(
                "row(time with time zone)",
                rowSignature(unnamedParameter(signature("time with time zone"))));
        assertRowSignature(
                "row(timestamp with time zone)",
                rowSignature(unnamedParameter(signature("timestamp with time zone"))));
        assertRowSignature(
                "row(interval day to second)",
                rowSignature(unnamedParameter(signature("interval day to second"))));
        assertRowSignature(
                "row(interval year to month)",
                rowSignature(unnamedParameter(signature("interval year to month"))));
        assertRowSignature(
                "row(double precision)",
                rowSignature(unnamedParameter(signature("double"))));
        assertRowSignature(
                "row(array(time with time zone))",
                rowSignature(unnamedParameter(array(signature("time with time zone")))));
        assertRowSignature(
                "row(map(timestamp with time zone,interval day to second))",
                rowSignature(unnamedParameter(map(signature("timestamp with time zone"), signature("interval day to second")))));

        // quoted field names
        assertRowSignature(
                "row(\"time with time zone\" time with time zone,\"double\" double)",
                rowSignature(
                        namedParameter("time with time zone", signature("time with time zone")),
                        namedParameter("double", signature("double"))));

        // allow spaces
        assertSignature(
                "row( time  time with time zone, array( interval day to second ) )",
                "row",
                ImmutableList.of("\"time\" time with time zone", "array(interval day to second)"),
                "row(\"time\" time with time zone,array(interval day to second))");

        // preserve base name case
        assertRowSignature(
                "RoW(a bigint,b varchar)",
                rowSignature(namedParameter("a", signature("bigint")), namedParameter("b", varchar())));
    }

    private TypeDescriptor varchar()
    {
        return new TypeDescriptor(StandardTypes.VARCHAR, TypeParameter.numericParameter(VarcharType.UNBOUNDED_LENGTH));
    }

    private TypeDescriptor varchar(long length)
    {
        return new TypeDescriptor(StandardTypes.VARCHAR, TypeParameter.numericParameter(length));
    }

    private static TypeDescriptor rowSignature(Field... fields)
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

    private TypeDescriptor signature(String name)
    {
        return new TypeDescriptor(name);
    }

    @Test
    public void parseSignature()
    {
        assertSignature("boolean", "boolean", ImmutableList.of());
        assertSignature("varchar", "varchar", ImmutableList.of(Integer.toString(VarcharType.UNBOUNDED_LENGTH)));

        assertSignature("array(bigint)", "array", ImmutableList.of("bigint"));

        assertSignature("array(array(bigint))", "array", ImmutableList.of("array(bigint)"));
        assertSignature(
                "array(timestamp with time zone)",
                "array",
                ImmutableList.of("timestamp with time zone"));

        assertSignature(
                "map(bigint,bigint)",
                "map",
                ImmutableList.of("bigint", "bigint"));
        assertSignature(
                "map(bigint,array(bigint))",
                "map",
                ImmutableList.of("bigint", "array(bigint)"));
        assertSignature(
                "map(bigint,map(bigint,map(varchar,bigint)))",
                "map",
                ImmutableList.of("bigint", "map(bigint,map(varchar,bigint))"));

        assertSignatureFail("blah()");
        assertSignatureFail("array()");
        assertSignatureFail("map()");

        // ensure this is not treated as a row type
        assertSignature("rowxxx(a)", "rowxxx", ImmutableList.of("a"));
    }

    @Test
    public void parseWithLiteralParameters()
    {
        assertSignature("foo(42)", "foo", ImmutableList.of("42"));
        assertSignature("varchar(10)", "varchar", ImmutableList.of("10"));
    }

    @Test
    public void testVarchar()
    {
        assertThat(VARCHAR.getTypeDescriptor().toString()).isEqualTo("varchar");
        assertThat(createVarcharType(42).getTypeDescriptor().toString()).isEqualTo("varchar(42)");
        assertThat(VARCHAR.getTypeDescriptor()).isEqualTo(createUnboundedVarcharType().getTypeDescriptor());
        assertThat(createUnboundedVarcharType().getTypeDescriptor()).isEqualTo(VARCHAR.getTypeDescriptor());
        assertThat(VARCHAR.getTypeDescriptor().hashCode()).isEqualTo(createUnboundedVarcharType().getTypeDescriptor().hashCode());
        assertThat(createUnboundedVarcharType().getTypeDescriptor())
                .isNotEqualTo(createVarcharType(10).getTypeDescriptor());
    }

    private static void assertRowSignature(
            String typeName,
            TypeDescriptor expectedSignature)
    {
        TypeDescriptor signature = parseTypeDescriptor(typeName);
        assertThat(signature).isEqualTo(expectedSignature);
    }

    private static void assertSignature(String typeName, String base, List<String> parameters)
    {
        assertSignature(typeName, base, parameters, typeName);
    }

    private static void assertSignature(
            String typeName,
            String base,
            List<String> parameters,
            String expectedTypeName)
    {
        TypeDescriptor signature = parseTypeDescriptor(typeName);
        assertThat(signature.getBase()).isEqualTo(base);
        assertThat(signature.getParameters()).hasSize(parameters.size());
        for (int i = 0; i < signature.getParameters().size(); i++) {
            assertThat(signature.getParameters().get(i).toString()).isEqualTo(parameters.get(i));
        }
        assertThat(signature.toString()).isEqualTo(expectedTypeName);
    }

    private void assertSignatureFail(String typeName)
    {
        assertThatThrownBy(() -> parseTypeDescriptor(typeName))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line [1-9][0-9]*:[1-9][0-9]*: mismatched input '.*'\\. Expecting: .*");
    }

    record Field(Optional<String> name, TypeDescriptor type) {}
}
