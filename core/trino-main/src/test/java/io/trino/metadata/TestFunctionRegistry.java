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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.CustomFunctions;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.metadata.Signature.mangleOperatorName;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.metadata.Signature.unmangleOperator;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFunctionRegistry
{
    @Test
    public void testIdentityCast()
    {
        BoundSignature exactOperator = createTestMetadataManager().getCoercion(HYPER_LOG_LOG, HYPER_LOG_LOG).getSignature();
        assertEquals(exactOperator, new BoundSignature(mangleOperatorName(OperatorType.CAST), HYPER_LOG_LOG, ImmutableList.of(HYPER_LOG_LOG)));
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
            BoundSignature exactOperator = metadata.resolveOperator(operatorType, argumentTypes).getSignature();
            assertEquals(exactOperator.toSignature(), function.getSignature());
            foundOperator = true;
        }
        assertTrue(foundOperator);
    }

    @Test
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
        assertThatThrownBy(() -> metadata.addFunctions(functions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("\\QFunction already registered: custom_add(bigint,bigint):bigint\\E");
    }

    @Test
    public void testConflictingScalarAggregation()
    {
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(ScalarSum.class)
                .getFunctions();

        assertThatThrownBy(() -> createTestMetadataManager().addFunctions(functions))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("'sum' is both an aggregation and a scalar function");
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
                                ImmutableList.of(Signature.withVariadicBound("T1", "row"),
                                        Signature.withVariadicBound("T2", "row"),
                                        Signature.withVariadicBound("T3", "row"))))
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
                .returns(functionSignature("JoniRegExp"));

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
                .typeVariableConstraints(typeVariableConstraints);
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
            Signature actualSignature = resolveSignature().toSignature();
            assertEquals(actualSignature, expectedSignature);
            return this;
        }

        public ResolveFunctionAssertion failsWithMessage(String... messages)
        {
            assertThatThrownBy(this::resolveSignature)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContainingAll(messages);
            return this;
        }

        private BoundSignature resolveSignature()
        {
            Metadata metadata = createTestMetadataManager();
            metadata.addFunctions(createFunctionsFromSignatures());
            return metadata.resolveFunction(QualifiedName.of(TEST_FUNCTION_NAME), fromTypeSignatures(parameterTypes)).getSignature();
        }

        private List<SqlFunction> createFunctionsFromSignatures()
        {
            ImmutableList.Builder<SqlFunction> functions = ImmutableList.builder();
            for (SignatureBuilder functionSignature : functionSignatures) {
                Signature signature = functionSignature.name(TEST_FUNCTION_NAME).build();
                FunctionMetadata functionMetadata = new FunctionMetadata(
                        signature,
                        false,
                        nCopies(signature.getArgumentTypes().size(), new FunctionArgumentDefinition(false)),
                        false,
                        false,
                        "testing function that does nothing",
                        SCALAR);
                functions.add(new SqlScalarFunction(functionMetadata)
                {
                    @Override
                    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
                    {
                        return new ChoicesScalarFunctionImplementation(
                                functionBinding,
                                FAIL_ON_NULL,
                                nCopies(functionBinding.getArity(), NEVER_NULL),
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
