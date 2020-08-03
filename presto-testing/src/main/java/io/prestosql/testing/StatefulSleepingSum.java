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
package io.prestosql.testing;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Collections.nCopies;

public class StatefulSleepingSum
        extends SqlScalarFunction
{
    public static final StatefulSleepingSum STATEFUL_SLEEPING_SUM = new StatefulSleepingSum();

    private StatefulSleepingSum()
    {
        super(new FunctionMetadata(
                new Signature(
                        "stateful_sleeping_sum",
                        ImmutableList.of(typeVariable("bigint")),
                        ImmutableList.of(),
                        BIGINT.getTypeSignature(),
                        ImmutableList.of(DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                        false),
                false,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                true,
                true,
                "testing not thread safe function",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        int args = 4;
        return new ScalarFunctionImplementation(
                FAIL_ON_NULL,
                nCopies(args, NEVER_NULL),
                methodHandle(StatefulSleepingSum.class, "statefulSleepingSum", State.class, double.class, long.class, long.class, long.class),
                Optional.of(constructorMethodHandle(State.class)));
    }

    public static long statefulSleepingSum(State state, double sleepProbability, long sleepDurationMillis, long a, long b)
    {
        int currentThreads = state.currentThreads.incrementAndGet();
        try {
            checkState(currentThreads == 1, "%s threads concurrently executing a stateful function", currentThreads);
            if (ThreadLocalRandom.current().nextDouble() < sleepProbability) {
                Thread.sleep(sleepDurationMillis);
            }
            return a + b;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        finally {
            state.currentThreads.decrementAndGet();
        }
    }

    public static class State
    {
        private final AtomicInteger currentThreads = new AtomicInteger();
    }
}
