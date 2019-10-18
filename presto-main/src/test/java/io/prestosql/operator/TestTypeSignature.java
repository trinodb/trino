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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Lists.transform;
import static io.prestosql.operator.TypeSignatureParser.parseTypeSignature;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.TypeSignature.rowType;
import static io.prestosql.spi.type.TypeSignatureParameter.namedField;
import static io.prestosql.spi.type.TypeSignatureParameter.numericParameter;
import static io.prestosql.spi.type.TypeSignatureParameter.typeVariable;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeSignature
{
    @Test
    public void parseSignatureWithLiterals()
    {
        TypeSignature result = new TypeSignature("decimal", typeVariable("X"), numericParameter(42));
        assertEquals(result.getParameters().size(), 2);
        assertEquals(result.getParameters().get(0).isVariable(), true);
        assertEquals(result.getParameters().get(1).isLongLiteral(), true);
    }

    @Test
    public void parseRowSignature()
    {
        // row signature with named fields
        assertRowSignature(
                "row(a bigint,b varchar)",
                rowSignature(namedParameter("a", false, signature("bigint")), namedParameter("b", false, varchar())));
        assertRowSignature(
                "row(__a__ bigint,_b@_: _varchar)",
                rowSignature(namedParameter("__a__", false, signature("bigint")), namedParameter("_b@_:", false, signature("_varchar"))));
        assertRowSignature(
                "row(a bigint,b array(bigint),c row(a bigint))",
                rowSignature(
                        namedParameter("a", false, signature("bigint")),
                        namedParameter("b", false, array(signature("bigint"))),
                        namedParameter("c", false, rowSignature(namedParameter("a", false, signature("bigint"))))));
        assertRowSignature(
                "row(a varchar(10),b row(a bigint))",
                rowSignature(
                        namedParameter("a", false, varchar(10)),
                        namedParameter("b", false, rowSignature(namedParameter("a", false, signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,col1 double))",
                array(rowSignature(namedParameter("col0", false, signature("bigint")), namedParameter("col1", false, signature("double")))));
        assertRowSignature(
                "row(col0 array(row(col0 bigint,col1 double)))",
                rowSignature(namedParameter("col0", false, array(
                        rowSignature(namedParameter("col0", false, signature("bigint")), namedParameter("col1", false, signature("double")))))));
        assertRowSignature(
                "row(a decimal(p1,s1),b decimal(p2,s2))",
                ImmutableSet.of("p1", "s1", "p2", "s2"),
                rowSignature(namedParameter("a", false, decimal("p1", "s1")), namedParameter("b", false, decimal("p2", "s2"))));

        // row with mixed fields
        assertRowSignature(
                "row(bigint,varchar)",
                rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(varchar())));
        assertRowSignature(
                "row(bigint,array(bigint),row(a bigint))",
                rowSignature(
                        unnamedParameter(signature("bigint")),
                        unnamedParameter(array(signature("bigint"))),
                        unnamedParameter(rowSignature(namedParameter("a", false, signature("bigint"))))));
        assertRowSignature(
                "row(varchar(10),b row(bigint))",
                rowSignature(
                        unnamedParameter(varchar(10)),
                        namedParameter("b", false, rowSignature(unnamedParameter(signature("bigint"))))));
        assertRowSignature(
                "array(row(col0 bigint,double))",
                array(rowSignature(namedParameter("col0", false, signature("bigint")), unnamedParameter(signature("double")))));
        assertRowSignature(
                "row(col0 array(row(bigint,double)))",
                rowSignature(namedParameter("col0", false, array(
                        rowSignature(unnamedParameter(signature("bigint")), unnamedParameter(signature("double")))))));
        assertRowSignature(
                "row(a decimal(p1,s1),decimal(p2,s2))",
                ImmutableSet.of("p1", "s1", "p2", "s2"),
                rowSignature(namedParameter("a", false, decimal("p1", "s1")), unnamedParameter(decimal("p2", "s2"))));

        // named fields of types with spaces
        assertRowSignature(
                "row(time time with time zone)",
                rowSignature(namedParameter("time", false, signature("time with time zone"))));
        assertRowSignature(
                "row(time timestamp with time zone)",
                rowSignature(namedParameter("time", false, signature("timestamp with time zone"))));
        assertRowSignature(
                "row(interval interval day to second)",
                rowSignature(namedParameter("interval", false, signature("interval day to second"))));
        assertRowSignature(
                "row(interval interval year to month)",
                rowSignature(namedParameter("interval", false, signature("interval year to month"))));
        assertRowSignature(
                "row(double double precision)",
                rowSignature(namedParameter("double", false, signature("double precision"))));

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
                rowSignature(unnamedParameter(signature("double precision"))));
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
                        namedParameter("time with time zone", true, signature("time with time zone")),
                        namedParameter("double", true, signature("double"))));

        // allow spaces
        assertSignature(
                "row( time  time with time zone, array( interval day to seconds ) )",
                "row",
                ImmutableList.of("time time with time zone", "array(interval day to seconds)"),
                "row(time time with time zone,array(interval day to seconds))");

        // preserve base name case
        assertRowSignature(
                "RoW(a bigint,b varchar)",
                rowSignature(namedParameter("a", false, signature("bigint")), namedParameter("b", false, varchar())));

        // signature with invalid type
        assertRowSignature(
                "row(\"time\" with time zone)",
                rowSignature(namedParameter("time", true, signature("with time zone"))));
    }

    private TypeSignature varchar()
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.numericParameter(VarcharType.UNBOUNDED_LENGTH));
    }

    private TypeSignature varchar(long length)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.numericParameter(length));
    }

    private TypeSignature decimal(String precisionVariable, String scaleVariable)
    {
        return new TypeSignature(StandardTypes.DECIMAL, ImmutableList.of(
                typeVariable(precisionVariable), typeVariable(scaleVariable)));
    }

    private static TypeSignature rowSignature(NamedTypeSignature... columns)
    {
        return new TypeSignature("row", transform(asList(columns), TypeSignatureParameter::namedTypeParameter));
    }

    private static NamedTypeSignature namedParameter(String name, boolean delimited, TypeSignature value)
    {
        return new NamedTypeSignature(Optional.of(new RowFieldName(name, delimited)), value);
    }

    private static NamedTypeSignature unnamedParameter(TypeSignature value)
    {
        return new NamedTypeSignature(Optional.empty(), value);
    }

    private static TypeSignature array(TypeSignature type)
    {
        return new TypeSignature(StandardTypes.ARRAY, TypeSignatureParameter.typeParameter(type));
    }

    private static TypeSignature map(TypeSignature keyType, TypeSignature valueType)
    {
        return new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.typeParameter(keyType), TypeSignatureParameter.typeParameter(valueType));
    }

    private TypeSignature signature(String name)
    {
        return new TypeSignature(name);
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
                "map", ImmutableList.of("bigint", "array(bigint)"));
        assertSignature(
                "map(bigint,map(bigint,map(varchar,bigint)))",
                "map",
                ImmutableList.of("bigint", "map(bigint,map(varchar,bigint))"));

        assertSignatureFail("blah()");
        assertSignatureFail("array()");
        assertSignatureFail("map()");
        assertSignatureFail("x", ImmutableSet.of("x"));

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
        assertEquals(VARCHAR.getTypeSignature().toString(), "varchar");
        assertEquals(createVarcharType(42).getTypeSignature().toString(), "varchar(42)");
        assertEquals(VARCHAR.getTypeSignature(), createUnboundedVarcharType().getTypeSignature());
        assertEquals(createUnboundedVarcharType().getTypeSignature(), VARCHAR.getTypeSignature());
        assertEquals(VARCHAR.getTypeSignature().hashCode(), createUnboundedVarcharType().getTypeSignature().hashCode());
        assertNotEquals(createUnboundedVarcharType().getTypeSignature(), createVarcharType(10).getTypeSignature());
    }

    @Test
    public void testIsCalculated()
    {
        assertFalse(BIGINT.getTypeSignature().isCalculated());
        assertTrue(new TypeSignature("decimal", typeVariable("p"), typeVariable("s")).isCalculated());
        assertFalse(createDecimalType(2, 1).getTypeSignature().isCalculated());
        assertTrue(arrayType(new TypeSignature("decimal", typeVariable("p"), typeVariable("s"))).isCalculated());
        assertFalse(arrayType(createDecimalType(2, 1).getTypeSignature()).isCalculated());
        assertTrue(mapType(new TypeSignature("decimal", typeVariable("p1"), typeVariable("s1")), new TypeSignature("decimal", typeVariable("p2"), typeVariable("s2"))).isCalculated());
        assertFalse(mapType(createDecimalType(2, 1).getTypeSignature(), createDecimalType(3, 1).getTypeSignature()).isCalculated());
        assertTrue(rowType(namedField("a", new TypeSignature("decimal", typeVariable("p1"), typeVariable("s1"))), namedField("b", new TypeSignature("decimal", typeVariable("p2"), typeVariable("s2")))).isCalculated());
    }

    private static void assertRowSignature(
            String typeName,
            Set<String> literalParameters,
            TypeSignature expectedSignature)
    {
        TypeSignature signature = parseTypeSignature(typeName, literalParameters);
        assertEquals(signature, expectedSignature);
        assertEquals(signature.toString(), typeName);
    }

    private static void assertRowSignature(
            String typeName,
            TypeSignature expectedSignature)
    {
        assertRowSignature(typeName, ImmutableSet.of(), expectedSignature);
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
        TypeSignature signature = parseTypeSignature(typeName, ImmutableSet.of());
        assertEquals(signature.getBase(), base);
        assertEquals(signature.getParameters().size(), parameters.size());
        for (int i = 0; i < signature.getParameters().size(); i++) {
            assertEquals(signature.getParameters().get(i).toString(), parameters.get(i));
        }
        assertEquals(signature.toString(), expectedTypeName);
    }

    private void assertSignatureFail(String typeName)
    {
        try {
            parseTypeSignature(typeName, ImmutableSet.of());
            fail("Type signatures with zero parameters should fail to parse");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private void assertSignatureFail(String typeName, Set<String> literalCalculationParameters)
    {
        try {
            parseTypeSignature(typeName, literalCalculationParameters);
            fail("Type signatures with zero parameters should fail to parse");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }
}
