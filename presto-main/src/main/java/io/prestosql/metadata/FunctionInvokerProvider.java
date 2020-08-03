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
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

class FunctionInvokerProvider
{
    private final ScalarFunctionAdapter functionAdapter = new ScalarFunctionAdapter(UNSUPPORTED);

    public FunctionInvoker createFunctionInvoker(
            ScalarFunctionImplementation scalarFunctionImplementation,
            BoundSignature boundSignature,
            InvocationConvention expectedConvention)
    {
        List<Choice> choices = new ArrayList<>();
        for (ScalarImplementationChoice choice : scalarFunctionImplementation.getChoices()) {
            InvocationConvention callingConvention = choice.getInvocationConvention();
            if (functionAdapter.canAdapt(callingConvention, expectedConvention)) {
                choices.add(new Choice(choice, callingConvention));
            }
        }
        if (choices.isEmpty()) {
            throw new PrestoException(FUNCTION_NOT_FOUND,
                    format("Function implementation for (%s) cannot be adapted to convention (%s)", boundSignature, expectedConvention));
        }

        Choice bestChoice = Collections.max(choices, comparingInt(Choice::getScore));
        MethodHandle methodHandle = functionAdapter.adapt(
                bestChoice.getChoice().getMethodHandle(),
                boundSignature.getArgumentTypes(),
                bestChoice.getCallingConvention(),
                expectedConvention);
        return new FunctionInvoker(
                methodHandle,
                bestChoice.getChoice().getInstanceFactory(),
                bestChoice.getChoice().getLambdaInterfaces());
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
