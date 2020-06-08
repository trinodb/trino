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

import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentType.FUNCTION_TYPE;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

class FunctionInvokerProvider
{
    private final ScalarFunctionAdapter functionAdapter = new ScalarFunctionAdapter(UNSUPPORTED);
    private final Metadata metadata;

    public FunctionInvokerProvider(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public FunctionInvoker createFunctionInvoker(
            ScalarFunctionImplementation scalarFunctionImplementation,
            Signature resolvedSignature,
            InvocationConvention expectedConvention)
    {
        List<Choice> choices = new ArrayList<>();
        for (ScalarImplementationChoice choice : scalarFunctionImplementation.getAllChoices()) {
            InvocationConvention callingConvention = toCallingConvention(choice);
            if (functionAdapter.canAdapt(callingConvention, expectedConvention)) {
                choices.add(new Choice(choice, callingConvention));
            }
        }
        if (choices.isEmpty()) {
            throw new PrestoException(FUNCTION_NOT_FOUND,
                    format("Function implementation for (%s) cannot be adapted to convention (%s)", resolvedSignature, expectedConvention));
        }

        Choice bestChoice = Collections.max(choices, comparingInt(Choice::getScore));
        List<Type> actualTypes = resolvedSignature.getArgumentTypes().stream()
                .map(metadata::getType)
                .collect(toImmutableList());
        MethodHandle methodHandle = functionAdapter.adapt(bestChoice.getChoice().getMethodHandle(), actualTypes, bestChoice.getCallingConvention(), expectedConvention);
        return new FunctionInvoker(
                methodHandle,
                bestChoice.getChoice().getInstanceFactory(),
                bestChoice.getChoice().getArgumentProperties().stream()
                        .map(ArgumentProperty::getLambdaInterface)
                        .collect(Collectors.toList()));
    }

    private static InvocationConvention toCallingConvention(ScalarImplementationChoice choice)
    {
        return new InvocationConvention(
                choice.getArgumentProperties().stream()
                        .map(FunctionInvokerProvider::toArgumentConvention)
                        .collect(toImmutableList()),
                choice.isNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                choice.hasSession(),
                choice.getInstanceFactory().isPresent());
    }

    private static InvocationArgumentConvention toArgumentConvention(ArgumentProperty argumentProperty)
    {
        if (argumentProperty.getArgumentType() == FUNCTION_TYPE) {
            return FUNCTION;
        }
        switch (argumentProperty.getNullConvention()) {
            case RETURN_NULL_ON_NULL:
                return NEVER_NULL;
            case USE_BOXED_TYPE:
                return BOXED_NULLABLE;
            case USE_NULL_FLAG:
                return NULL_FLAG;
            case BLOCK_AND_POSITION:
                return BLOCK_POSITION;
            default:
                throw new IllegalArgumentException("Unsupported null convention: " + argumentProperty.getNullConvention());
        }
    }

    private static final class Choice
    {
        private final ScalarImplementationChoice choice;
        private final InvocationConvention callingConvention;
        private final int score;

        public Choice(ScalarImplementationChoice choice, InvocationConvention callingConvention)
        {
            this.choice = requireNonNull(choice, "choice is null");
            this.callingConvention = requireNonNull(callingConvention, "callingConvention is null");

            int score = 0;
            for (InvocationArgumentConvention argument : callingConvention.getArgumentConventions()) {
                if (argument == NULL_FLAG) {
                    score += 1;
                }
                else if (argument == BLOCK_POSITION) {
                    score += 1000;
                }
            }
            this.score = score;
        }

        public ScalarImplementationChoice getChoice()
        {
            return choice;
        }

        public InvocationConvention getCallingConvention()
        {
            return callingConvention;
        }

        public int getScore()
        {
            return score;
        }
    }
}
