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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.type.FunctionType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.operator.TypeSignatureParser.parseTypeSignature;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.TypeSignature.functionType;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.TypeSignature.rowType;
import static io.prestosql.spi.type.TypeSignatureParameter.anonymousField;
import static io.prestosql.spi.type.TypeSignatureParameter.numericParameter;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSignatureBinder
{
    private final Metadata metadata = createTestMetadataManager();

    @Test
    public void testBindLiteralForDecimal()
    {
        TypeSignature leftType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p1"), TypeSignatureParameter.typeVariable("s1"));
        TypeSignature rightType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p2"), TypeSignatureParameter.typeVariable("s2"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo(createDecimalType(2, 1), createDecimalType(1, 0))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "p1", 2L,
                                "s1", 1L,
                                "p2", 1L,
                                "s2", 0L)));
    }

    @Test
    public void testBindPartialDecimal()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("decimal", numericParameter(4), TypeSignatureParameter.typeVariable("s")))
                .build();

        assertThat(function)
                .boundTo(createDecimalType(2, 1))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("s", 1L)));

        function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), numericParameter(1)))
                .build();

        assertThat(function)
                .boundTo(createDecimalType(2, 0))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 3L)));

        assertThat(function)
                .boundTo(createDecimalType(2, 1))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 2L)));

        assertThat(function)
                .boundTo(BIGINT)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 20L)));
    }

    @Test
    public void testBindLiteralForVarchar()
    {
        TypeSignature leftType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));
        TypeSignature rightType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("y"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo(createVarcharType(42), createVarcharType(44))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "x", 42L,
                                "y", 44L)));

        assertThat(function)
                .boundTo(UNKNOWN, createVarcharType(44))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "x", 0L,
                                "y", 44L)));
    }

    @Test
    public void testBindLiteralForRepeatedVarcharWithReturn()
    {
        TypeSignature leftType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));
        TypeSignature rightType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo(createVarcharType(44), createVarcharType(44))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo(createVarcharType(44), createVarcharType(42))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo(createVarcharType(42), createVarcharType(44))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo(UNKNOWN, createVarcharType(44))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
    }

    @Test
    public void testBindLiteralForRepeatedDecimal()
    {
        TypeSignature leftType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), TypeSignatureParameter.typeVariable("s"));
        TypeSignature rightType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), TypeSignatureParameter.typeVariable("s"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo(createDecimalType(10, 5), createDecimalType(10, 5))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 5L)));
        assertThat(function)
                .boundTo(createDecimalType(10, 8), createDecimalType(9, 8))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 8L)));
        assertThat(function)
                .boundTo(createDecimalType(10, 2), createDecimalType(10, 8))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 16L, "s", 8L)));
        assertThat(function)
                .boundTo(UNKNOWN, createDecimalType(10, 5))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 5L)));
    }

    @Test
    public void testBindLiteralForRepeatedVarchar()
    {
        TypeSignature leftType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));
        TypeSignature rightType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));
        TypeSignature returnType = new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"));

        Signature function = functionSignature()
                .returnType(returnType)
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .withCoercion()
                .boundTo(ImmutableList.of(createVarcharType(3), createVarcharType(5)), createVarcharType(5))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 5L)));

        assertThat(function)
                .withCoercion()
                .boundTo(ImmutableList.of(createVarcharType(3), createVarcharType(5)), createVarcharType(6))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 6L)));
    }

    @Test
    public void testBindUnknown()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x")))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN)
                .fails();

        assertThat(function)
                .boundTo(UNKNOWN)
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindMixedLiteralAndTypeVariables()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .argumentTypes(arrayType(new TypeSignature("T")), arrayType(new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), TypeSignatureParameter.typeVariable("s"))))
                .build();

        assertThat(function)
                .boundTo(new ArrayType(createDecimalType(2, 1)), new ArrayType(createDecimalType(3, 1)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T", createDecimalType(2, 1)),
                        ImmutableMap.of(
                                "p", 3L,
                                "s", 1L)));
    }

    @Test
    public void testBindDifferentLiteralParameters()
    {
        TypeSignature argType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), TypeSignatureParameter.typeVariable("s"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(argType, argType)
                .build();

        assertThat(function)
                .boundTo(createDecimalType(2, 1), createDecimalType(3, 1))
                .fails();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoVariableReuseAcrossTypes()
    {
        TypeSignature leftType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p1"), TypeSignatureParameter.typeVariable("s"));
        TypeSignature rightType = new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p2"), TypeSignatureParameter.typeVariable("s"));

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo(createDecimalType(2, 1), createDecimalType(3, 1))
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindUnknownToDecimal()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("decimal", TypeSignatureParameter.typeVariable("p"), TypeSignatureParameter.typeVariable("s")))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "p", 1L,
                                "s", 0L)));
    }

    @Test
    public void testBindUnknownToConcreteArray()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(arrayType(BOOLEAN.getTypeSignature()))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN)
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindTypeVariablesBasedOnTheSecondArgument()
    {
        Signature function = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(arrayType(new TypeSignature("T")), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN, createDecimalType(2, 1))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", createDecimalType(2, 1)),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindParametricTypeParameterToUnknown()
    {
        Signature function = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(arrayType(new TypeSignature("T")))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN)
                .fails();

        assertThat(function)
                .withCoercion()
                .boundTo(UNKNOWN)
                .succeeds();
    }

    @Test
    public void testBindUnknownToTypeParameter()
    {
        Signature function = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo(UNKNOWN)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", UNKNOWN),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindDoubleToBigint()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature())
                .build();

        assertThat(function)
                .boundTo(DOUBLE, BIGINT)
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindVarcharTemplateStyle()
    {
        Signature function = functionSignature()
                .returnType(new TypeSignature("T2"))
                .argumentTypes(new TypeSignature("T1"))
                .typeVariableConstraints(ImmutableList.of(
                        new TypeVariableConstraint("T1", true, false, "varchar"),
                        new TypeVariableConstraint("T2", true, false, "varchar")))
                .build();

        assertThat(function)
                .boundTo(ImmutableList.of(createVarcharType(42)), createVarcharType(1))
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T1", createVarcharType(42),
                                "T2", createVarcharType(1)),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindVarchar()
    {
        Signature function = functionSignature()
                .returnType(createVarcharType(42).getTypeSignature())
                .argumentTypes(createVarcharType(42).getTypeSignature())
                .build();

        assertThat(function)
                .boundTo(ImmutableList.of(createVarcharType(1)), createVarcharType(1))
                .fails();

        assertThat(function)
                .boundTo(ImmutableList.of(createVarcharType(1)), createVarcharType(1))
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo(ImmutableList.of(createVarcharType(1)), createVarcharType(42))
                .withCoercion()
                .succeeds();

        assertThat(function)
                .boundTo(ImmutableList.of(createVarcharType(44)), createVarcharType(44))
                .withCoercion()
                .fails();
    }

    @Test
    public void testBindUnparametrizedVarchar()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x")))
                .build();

        assertThat(function)
                .boundTo(VARCHAR)
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", (long) Integer.MAX_VALUE)));
    }

    @Test
    public void testBindToUnparametrizedVarcharIsImpossible()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(VARCHAR.getTypeSignature())
                .build();

        assertThat(function)
                .boundTo(createVarcharType(3))
                .withCoercion()
                .succeeds();

        assertThat(function)
                .boundTo(UNKNOWN)
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBasic()
    {
        Signature function = functionSignature()
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .returnType(new TypeSignature("T"))
                .argumentTypes(new TypeSignature("T"))
                .build();

        assertThat(function)
                .boundTo(BIGINT)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo(VARCHAR)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", VARCHAR),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo(VARCHAR, BIGINT)
                .fails();

        assertThat(function)
                .boundTo(new ArrayType(BIGINT))
                .produces(new BoundVariables(
                        ImmutableMap.of("T", new ArrayType(BIGINT)),
                        ImmutableMap.of()));
    }

    @Test
    public void testMismatchedArgumentCount()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(BIGINT.getTypeSignature(), BIGINT.getTypeSignature())
                .build();

        assertThat(function)
                .boundTo(BIGINT, BIGINT, BIGINT)
                .fails();

        assertThat(function)
                .boundTo(BIGINT)
                .fails();
    }

    @Test
    public void testNonParametric()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(BIGINT.getTypeSignature())
                .build();

        assertThat(function)
                .boundTo(BIGINT)
                .succeeds();

        assertThat(function)
                .boundTo(VARCHAR)
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo(VARCHAR, BIGINT)
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo(new ArrayType(BIGINT))
                .withCoercion()
                .fails();
    }

    @Test
    public void testArray()
    {
        Signature getFunction = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(arrayType(new TypeSignature("T")))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(getFunction)
                .boundTo(new ArrayType(BIGINT))
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(getFunction)
                .boundTo(BIGINT)
                .withCoercion()
                .fails();

        assertThat(getFunction)
                .boundTo(RowType.anonymous(ImmutableList.of(BIGINT)))
                .withCoercion()
                .fails();

        Signature containsFunction = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(arrayType(new TypeSignature("T")), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertThat(containsFunction)
                .boundTo(new ArrayType(BIGINT), BIGINT)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(containsFunction)
                .boundTo(new ArrayType(BIGINT), VARCHAR)
                .withCoercion()
                .fails();

        assertThat(containsFunction)
                .boundTo(new ArrayType(HYPER_LOG_LOG), HYPER_LOG_LOG)
                .withCoercion()
                .fails();

        Signature castFunction = functionSignature()
                .returnType(arrayType(new TypeSignature("T2")))
                .argumentTypes(arrayType(new TypeSignature("T1")), arrayType(new TypeSignature("T2")))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T1"), typeVariable("T2")))
                .build();

        assertThat(castFunction)
                .boundTo(new ArrayType(UNKNOWN), new ArrayType(createDecimalType(2, 1)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T1", UNKNOWN,
                                "T2", createDecimalType(2, 1)),
                        ImmutableMap.of()));

        Signature fooFunction = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(arrayType(new TypeSignature("T")), arrayType(new TypeSignature("T")))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(fooFunction)
                .boundTo(new ArrayType(BIGINT), new ArrayType(BIGINT))
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(fooFunction)
                .boundTo(new ArrayType(BIGINT), new ArrayType(VARCHAR))
                .withCoercion()
                .fails();
    }

    @Test
    public void testMap()
    {
        Signature getValueFunction = functionSignature()
                .returnType(new TypeSignature("V"))
                .argumentTypes(mapType(new TypeSignature("K"), new TypeSignature("V")), new TypeSignature("K"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("K"), typeVariable("V")))
                .build();

        assertThat(getValueFunction)
                .boundTo(type(mapType(BIGINT.getTypeSignature(), VARCHAR.getTypeSignature())), BIGINT)
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "K", BIGINT,
                                "V", VARCHAR),
                        ImmutableMap.of()));

        assertThat(getValueFunction)
                .boundTo(type(mapType(BIGINT.getTypeSignature(), VARCHAR.getTypeSignature())), VARCHAR)
                .withCoercion()
                .fails();
    }

    @Test
    public void testRow()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(rowType(anonymousField(INTEGER.getTypeSignature())))
                .build();

        assertThat(function)
                .boundTo(RowType.anonymous(ImmutableList.of(TINYINT)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
        assertThat(function)
                .boundTo(RowType.anonymous(ImmutableList.of(INTEGER)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
        assertThat(function)
                .boundTo(RowType.anonymous(ImmutableList.of(BIGINT)))
                .withCoercion()
                .fails();

        Signature biFunction = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(rowType(anonymousField(new TypeSignature("T"))), rowType(anonymousField(new TypeSignature("T"))))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(biFunction)
                .boundTo(RowType.anonymous(ImmutableList.of(INTEGER)), RowType.anonymous(ImmutableList.of(BIGINT)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));
        assertThat(biFunction)
                .boundTo(RowType.anonymous(ImmutableList.of(INTEGER)), RowType.anonymous(ImmutableList.of(BIGINT)))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));
    }

    @Test
    public void testVariadic()
    {
        Signature mapVariadicBoundFunction = functionSignature()
                .returnType(BIGINT.getTypeSignature())
                .argumentTypes(new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "map")))
                .build();

        assertThat(mapVariadicBoundFunction)
                .boundTo(type(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature())))
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                        ImmutableMap.of()));

        assertThat(mapVariadicBoundFunction)
                .boundTo(new ArrayType(BIGINT))
                .fails();

        assertThat(mapVariadicBoundFunction)
                .boundTo(new ArrayType(BIGINT))
                .withCoercion()
                .fails();

        Signature decimalVariadicBoundFunction = functionSignature()
                .returnType(BIGINT.getTypeSignature())
                .argumentTypes(new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "decimal")))
                .build();

        assertThat(decimalVariadicBoundFunction)
                .boundTo(createDecimalType(2, 1))
                .produces(new BoundVariables(
                        ImmutableMap.of("T", createDecimalType(2, 1)),
                        ImmutableMap.of()));

        assertThat(decimalVariadicBoundFunction)
                .boundTo(BIGINT)
                .fails();
    }

    @Test
    public void testBindUnknownToVariadic()
    {
        Signature rowFunction = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "row")))
                .build();

        assertThat(rowFunction)
                .boundTo(UNKNOWN, RowType.from(ImmutableList.of(RowType.field("a", BIGINT))))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", RowType.from(ImmutableList.of(RowType.field("a", BIGINT)))),
                        ImmutableMap.of()));

        Signature arrayFunction = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "array")))
                .build();

        assertThat(arrayFunction)
                .boundTo(UNKNOWN, new ArrayType(BIGINT))
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", new ArrayType(BIGINT)),
                        ImmutableMap.of()));
    }

    @Test
    public void testVarArgs()
    {
        Signature variableArityFunction = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .setVariableArity(true)
                .build();

        assertThat(variableArityFunction)
                .boundTo(BIGINT)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo(VARCHAR)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", VARCHAR),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo(BIGINT, BIGINT)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo(BIGINT, VARCHAR)
                .withCoercion()
                .fails();
    }

    @Test
    public void testCoercion()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"), DOUBLE.getTypeSignature())
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo(DOUBLE, DOUBLE)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", DOUBLE),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo(BIGINT, BIGINT)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo(VARCHAR, BIGINT)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", VARCHAR),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo(BIGINT, VARCHAR)
                .withCoercion()
                .fails();
    }

    @Test
    public void testUnknownCoercion()
    {
        Signature foo = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(foo)
                .boundTo(UNKNOWN, UNKNOWN)
                .produces(new BoundVariables(
                        ImmutableMap.of("T", UNKNOWN),
                        ImmutableMap.of()));

        assertThat(foo)
                .boundTo(UNKNOWN, BIGINT)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(foo)
                .boundTo(VARCHAR, BIGINT)
                .withCoercion()
                .fails();

        Signature bar = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(new TypeSignature("T"), new TypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertThat(bar)
                .boundTo(UNKNOWN, BIGINT)
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", BIGINT),
                        ImmutableMap.of()));

        assertThat(bar)
                .boundTo(VARCHAR, BIGINT)
                .withCoercion()
                .fails();

        assertThat(bar)
                .boundTo(HYPER_LOG_LOG, HYPER_LOG_LOG)
                .withCoercion()
                .fails();
    }

    @Test
    public void testFunction()
    {
        Signature simple = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(functionType(INTEGER.getTypeSignature(), INTEGER.getTypeSignature()))
                .build();

        assertThat(simple)
                .boundTo(INTEGER)
                .fails();
        assertThat(simple)
                .boundTo(new FunctionType(ImmutableList.of(INTEGER), INTEGER))
                .succeeds();
        // TODO: Support coercion of return type of lambda
        assertThat(simple)
                .boundTo(new FunctionType(ImmutableList.of(INTEGER), SMALLINT))
                .withCoercion()
                .fails();
        assertThat(simple)
                .boundTo(new FunctionType(ImmutableList.of(INTEGER), BIGINT))
                .withCoercion()
                .fails();

        Signature applyTwice = functionSignature()
                .returnType(new TypeSignature("V"))
                .argumentTypes(
                        new TypeSignature("T"),
                        functionType(new TypeSignature("T"), new TypeSignature("U")),
                        functionType(new TypeSignature("U"), new TypeSignature("V")))
                .typeVariableConstraints(typeVariable("T"), typeVariable("U"), typeVariable("V"))
                .build();
        assertThat(applyTwice)
                .boundTo(INTEGER, INTEGER, INTEGER)
                .fails();
        assertThat(applyTwice)
                .boundTo(INTEGER, new FunctionType(ImmutableList.of(INTEGER), VARCHAR), new FunctionType(ImmutableList.of(VARCHAR), DOUBLE))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("U", VARCHAR)
                        .setTypeVariable("V", DOUBLE)
                        .build());
        assertThat(applyTwice)
                .boundTo(
                        INTEGER,
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(INTEGER), VARCHAR).getTypeSignature()),
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(VARCHAR), DOUBLE).getTypeSignature()))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("U", VARCHAR)
                        .setTypeVariable("V", DOUBLE)
                        .build());
        assertThat(applyTwice)
                .boundTo(
                        // pass function argument to non-function position of a function
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(INTEGER), VARCHAR).getTypeSignature()),
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(INTEGER), VARCHAR).getTypeSignature()),
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(VARCHAR), DOUBLE).getTypeSignature()))
                .fails();
        assertThat(applyTwice)
                .boundTo(
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(INTEGER), VARCHAR).getTypeSignature()),
                        // pass non-function argument to function position of a function
                        INTEGER,
                        new TypeSignatureProvider(functionArgumentTypes -> new FunctionType(ImmutableList.of(VARCHAR), DOUBLE).getTypeSignature()))
                .fails();

        Signature flatMap = functionSignature()
                .returnType(arrayType(new TypeSignature("T")))
                .argumentTypes(arrayType(new TypeSignature("T")), functionType(new TypeSignature("T"), arrayType(new TypeSignature("T"))))
                .typeVariableConstraints(typeVariable("T"))
                .build();
        assertThat(flatMap)
                .boundTo(new ArrayType(INTEGER), new FunctionType(ImmutableList.of(INTEGER), new ArrayType(INTEGER)))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .build());

        Signature varargApply = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(new TypeSignature("T"), functionType(new TypeSignature("T"), new TypeSignature("T")))
                .typeVariableConstraints(typeVariable("T"))
                .setVariableArity(true)
                .build();
        assertThat(varargApply)
                .boundTo(INTEGER, new FunctionType(ImmutableList.of(INTEGER), INTEGER), new FunctionType(ImmutableList.of(INTEGER), INTEGER), new FunctionType(ImmutableList.of(INTEGER), INTEGER))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .build());
        assertThat(varargApply)
                .boundTo(INTEGER, new FunctionType(ImmutableList.of(INTEGER), INTEGER), new FunctionType(ImmutableList.of(INTEGER), DOUBLE), new FunctionType(ImmutableList.of(DOUBLE), DOUBLE))
                .fails();

        Signature loop = functionSignature()
                .returnType(new TypeSignature("T"))
                .argumentTypes(
                        new TypeSignature("T"),
                        functionType(new TypeSignature("T"), new TypeSignature("T")))
                .typeVariableConstraints(typeVariable("T"))
                .build();
        assertThat(loop)
                .boundTo(INTEGER, new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, BIGINT).getTypeSignature()))
                .fails();
        assertThat(loop)
                .boundTo(INTEGER, new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, BIGINT).getTypeSignature()))
                .withCoercion()
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", BIGINT)
                        .build());
        // TODO: Support coercion of return type of lambda
        assertThat(loop)
                .withCoercion()
                .boundTo(INTEGER, new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, SMALLINT).getTypeSignature()))
                .fails();

        // TODO: Support coercion of return type of lambda
        // Without coercion support for return type of lambda, the return type of lambda must be `varchar(x)` to avoid need for coercions.
        Signature varcharApply = functionSignature()
                .returnType(VARCHAR.getTypeSignature())
                .argumentTypes(VARCHAR.getTypeSignature(), functionType(VARCHAR.getTypeSignature(), new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"))))
                .build();
        assertThat(varcharApply)
                .withCoercion()
                .boundTo(createVarcharType(10), new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, createVarcharType(1)).getTypeSignature()))
                .succeeds();

        Signature sortByKey = functionSignature()
                .returnType(arrayType(new TypeSignature("T")))
                .argumentTypes(
                        arrayType(new TypeSignature("T")),
                        functionType(new TypeSignature("T"), new TypeSignature("E")))
                .typeVariableConstraints(typeVariable("T"), orderableTypeParameter("E"))
                .build();
        assertThat(sortByKey)
                .boundTo(new ArrayType(INTEGER), new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, VARCHAR).getTypeSignature()))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("E", VARCHAR)
                        .build());
    }

    @Test
    public void testBindParameters()
    {
        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T1", DOUBLE)
                .setTypeVariable("T2", BIGINT)
                .setTypeVariable("T3", createDecimalType(5, 3))
                .setLongVariable("p", 1L)
                .setLongVariable("s", 2L)
                .build();

        assertThat("bigint", boundVariables, "bigint");
        assertThat("T1", boundVariables, "double");
        assertThat("T2", boundVariables, "bigint");
        assertThat("array(T1)", boundVariables, "array(double)");
        assertThat("array(T3)", boundVariables, "array(decimal(5,3))");
        assertThat("map(T1,T2)", boundVariables, "map(double,bigint)");
        assertThat("bla(T1,42,T2)", boundVariables, "bla(double,42,bigint)");
        assertThat("varchar(p)", boundVariables, "varchar(1)");
        assertThat("char(p)", boundVariables, "char(1)");
        assertThat("decimal(p,s)", boundVariables, "decimal(1,2)");
        assertThat("array(decimal(p,s))", boundVariables, "array(decimal(1,2))");

        assertBindVariablesFails("T1(bigint)", boundVariables, "Unbounded parameters can not have parameters");
    }

    private static void assertBindVariablesFails(String typeSignature, BoundVariables boundVariables, String reason)
    {
        try {
            SignatureBinder.applyBoundVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables);
            fail(reason);
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private static void assertThat(String typeSignature, BoundVariables boundVariables, String expectedTypeSignature)
    {
        assertEquals(
                SignatureBinder.applyBoundVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables).toString(),
                expectedTypeSignature);
    }

    private static SignatureBuilder functionSignature()
    {
        return new SignatureBuilder()
                .name("function")
                .kind(SCALAR);
    }

    private Type type(TypeSignature signature)
    {
        return requireNonNull(metadata.getType(signature));
    }

    private BindSignatureAssertion assertThat(Signature function)
    {
        return new BindSignatureAssertion(function);
    }

    private class BindSignatureAssertion
    {
        private final Signature function;
        private List<TypeSignatureProvider> argumentTypes;
        private Type returnType;
        private boolean allowCoercion;

        private BindSignatureAssertion(Signature function)
        {
            this.function = function;
        }

        public BindSignatureAssertion withCoercion()
        {
            allowCoercion = true;
            return this;
        }

        public BindSignatureAssertion boundTo(Object... arguments)
        {
            ImmutableList.Builder<TypeSignatureProvider> builder = ImmutableList.builder();
            for (Object argument : arguments) {
                if (argument instanceof Type) {
                    builder.add(new TypeSignatureProvider(((Type) argument).getTypeSignature()));
                    continue;
                }
                if (argument instanceof TypeSignatureProvider) {
                    builder.add((TypeSignatureProvider) argument);
                    continue;
                }
                throw new IllegalArgumentException(format("argument is of type %s. It should be Type or TypeSignatureProvider", argument.getClass()));
            }
            this.argumentTypes = builder.build();
            return this;
        }

        public BindSignatureAssertion boundTo(List<Type> arguments, Type returnType)
        {
            this.argumentTypes = fromTypes(arguments);
            this.returnType = returnType;
            return this;
        }

        public BindSignatureAssertion succeeds()
        {
            assertTrue(bindVariables().isPresent());
            return this;
        }

        public BindSignatureAssertion fails()
        {
            assertFalse(bindVariables().isPresent());
            return this;
        }

        public BindSignatureAssertion produces(BoundVariables expected)
        {
            Optional<BoundVariables> actual = bindVariables();
            assertTrue(actual.isPresent());
            assertEquals(actual.get(), expected);
            return this;
        }

        private Optional<BoundVariables> bindVariables()
        {
            assertNotNull(argumentTypes);
            SignatureBinder signatureBinder = new SignatureBinder(metadata, function, allowCoercion);
            if (returnType == null) {
                return signatureBinder.bindVariables(argumentTypes);
            }
            else {
                return signatureBinder.bindVariables(argumentTypes, returnType.getTypeSignature());
            }
        }
    }
}
