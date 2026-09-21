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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

/// Fuzz test guarding the [io.trino.spi.function.FunctionNullability] contract that the optimizer
/// relies on (e.g. [io.trino.sql.ir.IrExpressions#mayBeNull] and
/// [io.trino.sql.ir.optimizer.rule.EvaluateCallWithNullInput]): a scalar function that declares
/// `returnNullable == false` must never return SQL null when invoked on non-null inputs.
///
/// Coverage is best-effort — functions that are generic, variable-arity, take function-typed
/// arguments, or whose argument types can't be randomly populated are skipped, and any invocation
/// that throws is tolerated (throwing is not returning null). The check applies only to the concrete
/// overloads that survive that filter.
public class TestFuzzingFunctionNullability
{
    private static final int POSITIONS_PER_FUNCTION = 200;
    private static final float NULL_RATE = 0.1f;

    @Test
    public void testNonNullableReturnFunctionsNeverReturnNull()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        PlannerContext plannerContext = functionResolution.getPlannerContext();
        TypeManager typeManager = plannerContext.getTypeManager();
        InterpretedFunctionInvoker invoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
        ConnectorSession session = TEST_SESSION.toConnectorSession();

        int exercised = 0;
        for (FunctionMetadata function : functionResolution.listGlobalFunctions()) {
            if (function.getKind() != FunctionKind.SCALAR || !function.isDeterministic()) {
                continue;
            }

            Signature signature = function.getSignature();
            if (signature.isGeneric() || signature.isVariableArity()) {
                // Requires binding type variables / arbitrary arity to exercise; out of scope.
                continue;
            }

            List<Type> argumentTypes = resolveArgumentTypes(typeManager, signature);
            if (argumentTypes == null) {
                continue;
            }

            ResolvedFunction resolved;
            try {
                resolved = functionResolution.resolveFunction(function.getCanonicalName(), fromTypes(argumentTypes));
            }
            catch (RuntimeException _) {
                // Operators and internal functions aren't resolvable by canonical name; skip.
                continue;
            }

            if (resolved.functionNullability().isReturnNullable()) {
                continue;
            }

            exercised++;
            fuzzFunction(invoker, session, resolved, argumentTypes);
        }

        // Guard against the filter silently excluding everything (e.g. after an API change).
        assertThat(exercised)
                .describedAs("number of non-nullable-return scalar functions exercised")
                .isGreaterThan(150);
    }

    private static void fuzzFunction(InterpretedFunctionInvoker invoker, ConnectorSession session, ResolvedFunction resolved, List<Type> argumentTypes)
    {
        // One random block per argument, sharing null positions across the argument set, then exercise
        // every position - including those where nullable arguments carry nulls.
        List<Block> argumentBlocks = argumentTypes.stream()
                .map(type -> randomBlock(type, POSITIONS_PER_FUNCTION, NULL_RATE))
                .collect(toImmutableList());

        for (int position = 0; position < POSITIONS_PER_FUNCTION; position++) {
            List<Object> arguments = new ArrayList<>(argumentTypes.size());
            boolean nullOnNonNullableArgument = false;
            for (int argument = 0; argument < argumentTypes.size(); argument++) {
                Object value = readNativeValue(argumentTypes.get(argument), argumentBlocks.get(argument), position);
                // A null on a non-nullable argument short-circuits to a null result without invoking the
                // function, so it can't reveal a return-nullability violation.
                if (value == null && !resolved.functionNullability().isArgumentNullable(argument)) {
                    nullOnNonNullableArgument = true;
                }
                arguments.add(value);
            }
            if (nullOnNonNullableArgument) {
                continue;
            }

            Object result;
            try {
                result = invoker.invoke(resolved, session, arguments);
            }
            catch (Throwable _) {
                // A thrown error is not a null return; the non-nullable contract still holds.
                continue;
            }

            assertThat(result)
                    .describedAs("%s declares a non-nullable return but returned null for arguments %s", resolved.signature(), arguments)
                    .isNotNull();
        }
    }

    /// Builds a random block for the type. Integer-family values are bounded to a small range: some
    /// functions size their output by an integer argument (e.g. `bar(value, width)`), and an
    /// unbounded random width would allocate enough memory to trip `-XX:+ExitOnOutOfMemoryError`
    /// before the invocation's error handling can run.
    private static Block randomBlock(Type type, int positionCount, float nullRate)
    {
        if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT) {
            BlockBuilder builder = type.createBlockBuilder(null, positionCount);
            Random random = ThreadLocalRandom.current();
            for (int position = 0; position < positionCount; position++) {
                if (random.nextFloat() < nullRate) {
                    builder.appendNull();
                }
                else {
                    type.writeLong(builder, random.nextInt(201) - 100);
                }
            }
            return builder.build();
        }
        return createRandomBlockForType(type, positionCount, nullRate);
    }

    /// Resolves the ground argument types of a non-generic signature, or {@code null} if any argument
    /// type can't be built or randomly populated.
    private static List<Type> resolveArgumentTypes(TypeManager typeManager, Signature signature)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TypeTemplate template : signature.getArgumentTypes()) {
            Type type;
            try {
                type = typeManager.getType(TypeTemplates.toTypeDescriptor(template));
            }
            catch (RuntimeException _) {
                return null;
            }
            // Lambda arguments can't be fuzzed with random values.
            if (type instanceof FunctionType) {
                return null;
            }
            // Ensure a random value can be produced for this type before committing to fuzz it.
            try {
                createRandomBlockForType(type, 1, 0.0f);
            }
            catch (RuntimeException | Error _) {
                return null;
            }
            types.add(type);
        }
        return types.build();
    }
}
