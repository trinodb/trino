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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
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
                new FunctionNullability(false, ImmutableList.of(false, false, false, false)),
                true,
                true,
                "testing not thread safe function",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        int args = 4;
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
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
