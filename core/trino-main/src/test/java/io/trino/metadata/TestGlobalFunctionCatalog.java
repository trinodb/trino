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
import io.trino.FeaturesConfig;
import io.trino.client.NodeVersion;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.TypeVariableConstraint.typeVariable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGlobalFunctionCatalog
{
    @Test
    public void testIdentityCast()
    {
        BoundSignature exactOperator = new TestingFunctionResolution().getCoercion(HYPER_LOG_LOG, HYPER_LOG_LOG).signature();
        assertThat(exactOperator).isEqualTo(new BoundSignature(builtinFunctionName(CAST), HYPER_LOG_LOG, ImmutableList.of(HYPER_LOG_LOG)));
    }

    @Test
    public void testExactMatchBeforeCoercion()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        boolean foundOperator = false;
        for (FunctionMetadata function : listOperators(functionResolution)) {
            OperatorType operatorType = unmangleOperator(function.getCanonicalName());
            if (operatorType == CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST) {
                continue;
            }
            if (!function.getSignature().getTypeVariableConstraints().isEmpty()) {
                continue;
            }
            if (function.getSignature().getArgumentTypes().stream().anyMatch(TypeSignature::isCalculated)) {
                continue;
            }
            List<Type> argumentTypes = function.getSignature().getArgumentTypes().stream()
                    .map(functionResolution.getPlannerContext().getTypeManager()::getType)
                    .collect(toImmutableList());
            BoundSignature exactOperator = functionResolution.resolveOperator(operatorType, argumentTypes).signature();
            assertThat(exactOperator.toSignature()).isEqualTo(function.getSignature());
            foundOperator = true;
        }
        assertThat(foundOperator).isTrue();
    }

    @Test
    public void testDuplicateFunctions()
    {
        FunctionBundle functionBundle = extractFunctions(CustomAdd.class);

        TypeOperators typeOperators = new TypeOperators();
        GlobalFunctionCatalog globalFunctionCatalog = new GlobalFunctionCatalog(
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); });
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), NodeVersion.UNKNOWN));
        globalFunctionCatalog.addFunctions(functionBundle);
        assertThatThrownBy(() -> globalFunctionCatalog.addFunctions(functionBundle))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("\\QFunction already registered: custom_add(bigint,bigint):bigint\\E");
    }

    @Test
    public void testConflictingScalarAggregation()
    {
        FunctionBundle functions = extractFunctions(ScalarSum.class);

        TypeOperators typeOperators = new TypeOperators();
        GlobalFunctionCatalog globalFunctionCatalog = new GlobalFunctionCatalog(
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); },
                () -> { throw new UnsupportedOperationException(); });
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), NodeVersion.UNKNOWN));
        assertThatThrownBy(() -> globalFunctionCatalog.addFunctions(functions))
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
                        functionSignature("decimal(p,s)").variableArity())
                .forParameters(BIGINT, BIGINT, BIGINT)
                .returns(functionSignature("decimal(19,0)", "decimal(19,0)", "decimal(19,0)"));

        assertThatResolveFunction()
                .among(
                        functionSignature("double", "double", "double"),
                        functionSignature("bigint").variableArity())
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
                                ImmutableList.of(
                                        TypeVariableConstraint.builder("T1").variadicBound("row").build(),
                                        TypeVariableConstraint.builder("T2").variadicBound("row").build(),
                                        TypeVariableConstraint.builder("T3").variadicBound("row").build())))
                .forParameters(UnknownType.UNKNOWN, BIGINT, BIGINT)
                .returns(functionSignature("bigint", "bigint", "bigint"));
    }

    @Test
    public void testResolveFunctionForUnknown()
    {
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"))
                .forParameters(UnknownType.UNKNOWN)
                .returns(functionSignature("bigint"));

        // when coercion between the types exist, and the most specific function can be determined with the main algorithm
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint"),
                        functionSignature("integer"))
                .forParameters(UnknownType.UNKNOWN)
                .returns(functionSignature("integer"));

        // function that requires only unknown coercion must be preferred
        assertThatResolveFunction()
                .among(
                        functionSignature("bigint", "bigint"),
                        functionSignature("integer", "integer"))
                .forParameters(UnknownType.UNKNOWN, BIGINT)
                .returns(functionSignature("bigint", "bigint"));

        // when coercion between the types doesn't exist, but the return type is the same, so the random function must be chosen
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "boolean"),
                        functionSignature(ImmutableList.of("integer"), "boolean"))
                .forParameters(UnknownType.UNKNOWN)
                // any function can be selected, but to make it deterministic we sort function signatures alphabetically
                .returns(functionSignature("JoniRegExp"));

        // when the return type is different
        assertThatResolveFunction()
                .among(
                        functionSignature(ImmutableList.of("JoniRegExp"), "JoniRegExp"),
                        functionSignature(ImmutableList.of("integer"), "integer"))
                .forParameters(UnknownType.UNKNOWN)
                .failsWithMessage("Could not choose a best candidate operator. Explicit type casts must be added.");
    }

    private static List<FunctionMetadata> listOperators(TestingFunctionResolution functionResolution)
    {
        Set<String> operatorNames = Arrays.stream(OperatorType.values())
                .map(OperatorNameUtil::mangleOperatorName)
                .collect(toImmutableSet());

        return functionResolution.listGlobalFunctions().stream()
                .filter(function -> operatorNames.contains(function.getCanonicalName()))
                .collect(toImmutableList());
    }

    private static Signature.Builder functionSignature(String... argumentTypes)
    {
        return functionSignature(ImmutableList.copyOf(argumentTypes), "boolean");
    }

    private static Signature.Builder functionSignature(List<String> arguments, String returnType)
    {
        return functionSignature(arguments, returnType, ImmutableList.of());
    }

    private static Signature.Builder functionSignature(List<String> arguments, String returnType, List<TypeVariableConstraint> typeVariableConstraints)
    {
        Set<String> literalParameters = ImmutableSet.of("p", "s", "p1", "s1", "p2", "s2", "p3", "s3");
        List<TypeSignature> argumentSignatures = arguments.stream()
                .map(signature -> parseTypeSignature(signature, literalParameters))
                .collect(toImmutableList());
        return Signature.builder()
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

        private List<Signature.Builder> functionSignatures = ImmutableList.of();
        private List<TypeSignature> parameterTypes = ImmutableList.of();

        public ResolveFunctionAssertion among(Signature.Builder... functionSignatures)
        {
            this.functionSignatures = ImmutableList.copyOf(functionSignatures);
            return this;
        }

        public ResolveFunctionAssertion forParameters(Type... parameters)
        {
            this.parameterTypes = parseTypeSignatures(parameters);
            return this;
        }

        public ResolveFunctionAssertion returns(Signature.Builder functionSignature)
        {
            Signature expectedSignature = functionSignature.build();
            Signature actualSignature = resolveSignature().toSignature();
            assertThat(actualSignature).isEqualTo(expectedSignature);
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
            return new TestingFunctionResolution(createFunctionsFromSignatures())
                    .resolveFunction(TEST_FUNCTION_NAME, fromTypeSignatures(parameterTypes))
                    .signature();
        }

        private InternalFunctionBundle createFunctionsFromSignatures()
        {
            ImmutableList.Builder<SqlFunction> functions = ImmutableList.builder();
            for (Signature.Builder functionSignature : functionSignatures) {
                FunctionMetadata functionMetadata = FunctionMetadata.scalarBuilder(TEST_FUNCTION_NAME)
                        .signature(functionSignature.build())
                        .nondeterministic()
                        .description("testing function that does nothing")
                        .build();
                functions.add(new SqlScalarFunction(functionMetadata)
                {
                    @Override
                    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
                    {
                        return new ChoicesSpecializedSqlScalarFunction(
                                boundSignature,
                                FAIL_ON_NULL,
                                nCopies(boundSignature.getArity(), NEVER_NULL),
                                MethodHandles.identity(Void.class));
                    }
                });
            }
            return new InternalFunctionBundle(functions.build());
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

    public static final class CustomAdd
    {
        private CustomAdd() {}

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long customAdd(@SqlType(StandardTypes.BIGINT) long x, @SqlType(StandardTypes.BIGINT) long y)
        {
            return x + y;
        }
    }
}
