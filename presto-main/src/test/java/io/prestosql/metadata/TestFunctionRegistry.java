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
import com.google.common.collect.ImmutableSet;
import io.prestosql.operator.scalar.CustomFunctions;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.metadata.Signature.mangleOperatorName;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.metadata.Signature.unmangleOperator;
import static io.prestosql.operator.TypeSignatureParser.parseTypeSignature;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFunctionRegistry
{
    @Test
    public void testIdentityCast()
    {
        Signature exactOperator = createTestMetadataManager().getCoercion(HYPER_LOG_LOG, HYPER_LOG_LOG).getSignature();
        assertEquals(exactOperator.getName(), mangleOperatorName(OperatorType.CAST));
        assertEquals(
                exactOperator.getArgumentTypes().stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()),
                ImmutableList.of(StandardTypes.HYPER_LOG_LOG));
        assertEquals(exactOperator.getReturnType().getBase(), StandardTypes.HYPER_LOG_LOG);
    }

    @Test
    public void testExactMatchBeforeCoercion()
    {
        Metadata metadata = createTestMetadataManager();
        boolean foundOperator = false;
        for (FunctionMetadata function : listOperators(metadata)) {
            OperatorType operatorType = unmangleOperator(function.getSignature().getName());
            if (operatorType == OperatorType.CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST) {
                continue;
            }
            if (!function.getSignature().getTypeVariableConstraints().isEmpty()) {
                continue;
            }
            if (function.getSignature().getArgumentTypes().stream().anyMatch(TypeSignature::isCalculated)) {
                continue;
            }
            List<Type> argumentTypes = function.getSignature().getArgumentTypes().stream()
                    .map(metadata::getType)
                    .collect(toImmutableList());
            Signature exactOperator = metadata.resolveOperator(operatorType, argumentTypes).getSignature();
            assertEquals(exactOperator, function.getSignature());
            foundOperator = true;
        }
        assertTrue(foundOperator);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(CustomFunctions.class)
                .getFunctions()
                .stream()
                .filter(input -> input.getFunctionMetadata().getSignature().getName().equals("custom_add"))
                .collect(toImmutableList());

        Metadata metadata = createTestMetadataManager();
        metadata.addFunctions(functions);
        metadata.addFunctions(functions);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(ScalarSum.class)
                .getFunctions();

        createTestMetadataManager().addFunctions(functions);
    }

    @Test
    public void testResolveFunctionByExactMatch()
    {
        assertThatResolveFunction()
                .among(functionSignature("bigint", "bigint"))
                .forParameters(BIGINT, BIGINT)
                .returns(functionSignature("bigint", "bigint"));
    }

    @Test
    public void testResolveTypeParametrizedFunction()
    {
        assertThatResolveFunction()
                .among(functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))))
                .forParameters(BIGINT, BIGINT)
                .returns(functionSignature("bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionWithCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "double"),
                        functionSignature("decimal(p,s)", "decimal(p,s)"),
                        functionSignature("double", "double"))
                .forParameters(BIGINT, BIGINT)
                .returns(functionSignature("decimal(19,0)", "decimal(19,0)"));
    }

    @Test
    public void testAmbiguousCallWithNoCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "decimal(p,s)"),
                        functionSignature(ImmutableList.of("T", "T"), "boolean", ImmutableList.of(typeVariable("T"))))
                .forParameters(createDecimalType(3, 1), createDecimalType(3, 1))
                .returns(functionSignature("decimal(3,1)", "decimal(3,1)"));
    }

    @Test
    public void testAmbiguousCallWithCoercion()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("decimal(p,s)", "double"),
                        functionSignature("double", "decimal(p,s)"))
                .forParameters(BIGINT, BIGINT)
                .failsWithMessage("Could not choose a best candidate operator. Explicit type casts must be added.");
    }

    @Test
    public void testResolveFunctionWithCoercionInTypes()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("array(decimal(p,s))", "array(double)"),
                        functionSignature("array(decimal(p,s))", "array(decimal(p,s))"),
                        functionSignature("array(double)", "array(double)"))
                .forParameters(new ArrayType(BIGINT), new ArrayType(BIGINT))
                .returns(functionSignature("array(decimal(19,0))", "array(decimal(19,0))"));
    }

    @Test
    public void testResolveFunctionWithVariableArity()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("double", "double", "double"),
                        functionSignature("decimal(p,s)").setVariableArity(true))
                .forParameters(BIGINT, BIGINT, BIGINT)
                .returns(functionSignature("decimal(19,0)", "decimal(19,0)", "decimal(19,0)"));

        assertThatResolveFunction()
                .among(
                        functionSignature("double", "double", "double"),
                        functionSignature("bigint").setVariableArity(true))
                .forParameters(BIGINT, BIGINT, BIGINT)
                .returns(functionSignature("bigint", "bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionWithVariadicBound()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint", "bigint", "bigint"),
                        functionSignature(
                                ImmutableList.of("T1", "T2", "T3"),
                                "boolean",
                                ImmutableList.of(Signature.withVariadicBound("T1", "decimal"),
                                        Signature.withVariadicBound("T2", "decimal"),
                                        Signature.withVariadicBound("T3", "decimal"))))
                .forParameters(UNKNOWN, BIGINT, BIGINT)
                .returns(functionSignature("bigint", "bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionForUnknown()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"))
                .forParameters(UNKNOWN)
                .returns(functionSignature("bigint"));

        // when coercion between the types exist, and the most specific function can be determined with the main algorithm
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"),
                        functionSignature("integer"))
                .forParameters(UNKNOWN)
                .returns(functionSignature("integer"));

        // function that requires only unknown coercion must be preferred
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint", "bigint"),
                        functionSignature("integer", "integer"))
                .forParameters(UNKNOWN, BIGINT)
                .returns(functionSignature("bigint", "bigint"));

        // when coercion between the types doesn't exist, but the return type is the same, so the random function must be chosen
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "boolean"),
                        functionSignature(ImmutableList.of("integer"), "boolean"))
                .forParameters(UNKNOWN)
                // any function can be selected, but to make it deterministic we sort function signatures alphabetically
                .returns(functionSignature("integer"));

        // when the return type is different
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "JoniRegExp"),
                        functionSignature(ImmutableList.of("integer"), "integer"))
                .forParameters(UNKNOWN)
                .failsWithMessage("Could not choose a best candidate operator. Explicit type casts must be added.");
    }

    private static List<FunctionMetadata> listOperators(Metadata metadata)
    {
        Set<String> operatorNames = Arrays.stream(OperatorType.values())
                .map(Signature::mangleOperatorName)
                .collect(toImmutableSet());

        return metadata.listFunctions().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    private SignatureBuilder functionSignature(String... argumentTypes)
    {
        return functionSignature(ImmutableList.copyOf(argumentTypes), "boolean");
    }

    private static SignatureBuilder functionSignature(List<String> arguments, String returnType)
    {
        return functionSignature(arguments, returnType, ImmutableList.of());
    }

    private static SignatureBuilder functionSignature(List<String> arguments, String returnType, List<TypeVariableConstraint> typeVariableConstraints)
    {
        ImmutableSet<String> literalParameters = ImmutableSet.of("p", "s", "p1", "s1", "p2", "s2", "p3", "s3");
        List<TypeSignature> argumentSignatures = arguments.stream()
                .map((signature) -> parseTypeSignature(signature, literalParameters))
                .collect(toImmutableList());
        return new SignatureBuilder()
                .returnType(parseTypeSignature(returnType, literalParameters))
                .argumentTypes(argumentSignatures)
                .typeVariableConstraints(typeVariableConstraints)
                .kind(SCALAR);
    }

    private static ResolveFunctionAssertion assertThatResolveFunction()
    {
        return new ResolveFunctionAssertion();
    }

    private static class ResolveFunctionAssertion
    {
        private static final String TEST_FUNCTION_NAME = "TEST_FUNCTION_NAME";

        private List<SignatureBuilder> functionSignatures = ImmutableList.of();
        private List<TypeSignature> parameterTypes = ImmutableList.of();

        public ResolveFunctionAssertion among(SignatureBuilder... functionSignatures)
        {
            this.functionSignatures = ImmutableList.copyOf(functionSignatures);
            return this;
        }

        public ResolveFunctionAssertion forParameters(Type... parameters)
        {
            this.parameterTypes = parseTypeSignatures(parameters);
            return this;
        }

        public ResolveFunctionAssertion returns(SignatureBuilder functionSignature)
        {
            Signature expectedSignature = functionSignature.name(TEST_FUNCTION_NAME).build();
            Signature actualSignature = resolveSignature();
            assertEquals(actualSignature, expectedSignature);
            return this;
        }

        public ResolveFunctionAssertion failsWithMessage(String... messages)
        {
            try {
                resolveSignature();
                fail("didn't fail as expected");
            }
            catch (RuntimeException e) {
                String actualMessage = e.getMessage();
                for (String expectedMessage : messages) {
                    if (!actualMessage.contains(expectedMessage)) {
                        fail(format("%s doesn't contain %s", actualMessage, expectedMessage));
                    }
                }
            }
            return this;
        }

        private Signature resolveSignature()
        {
            Metadata metadata = createTestMetadataManager();
            metadata.addFunctions(createFunctionsFromSignatures());
            return metadata.resolveFunction(QualifiedName.of(TEST_FUNCTION_NAME), fromTypeSignatures(parameterTypes)).getSignature();
        }

        private List<SqlFunction> createFunctionsFromSignatures()
        {
            ImmutableList.Builder<SqlFunction> functions = ImmutableList.builder();
            for (SignatureBuilder functionSignature : functionSignatures) {
                FunctionMetadata functionMetadata = new FunctionMetadata(
                        functionSignature.name(TEST_FUNCTION_NAME).build(),
                        false,
                        false,
                        "testing function that does nothing");
                functions.add(new SqlScalarFunction(functionMetadata)
                {
                    @Override
                    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
                    {
                        return new ScalarFunctionImplementation(
                                false,
                                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                MethodHandles.identity(Void.class));
                    }
                });
            }
            return functions.build();
        }

        private static List<TypeSignature> parseTypeSignatures(Type... signatures)
        {
            return ImmutableList.copyOf(signatures)
                    .stream()
                    .map(Type::getTypeSignature)
                    .collect(toList());
        }
    }

    public static final class ScalarSum
    {
        private ScalarSum() {}

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long sum(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
        {
            return a + b;
        }
    }
}
