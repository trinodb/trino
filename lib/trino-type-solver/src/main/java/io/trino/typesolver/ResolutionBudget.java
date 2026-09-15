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
package io.trino.typesolver;

import io.trino.spi.TrinoException;

import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.TYPE_RESOLUTION_LIMIT_EXCEEDED;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;

/// Work accounting for one function resolution, including nested resolutions, candidate
/// attempts, subtype queries, and materialization. The scope is synchronous and confined
/// to the resolving thread. Exhaustion is an error, never a failed candidate match.
public final class ResolutionBudget
{
    private static final ScopedValue<ResolutionBudget> CURRENT = ScopedValue.newInstance();
    private static final long DEFAULT_WORK_LIMIT = 1_000_000;
    private static final int MAX_NESTING = 128;

    private final long workLimit;
    private long work;
    private int nesting;

    public ResolutionBudget(long workLimit)
    {
        if (workLimit < 1) {
            throw new IllegalArgumentException("workLimit must be positive");
        }
        this.workLimit = workLimit;
    }

    public static <T> T withBudget(Supplier<T> action)
    {
        return current().run(action);
    }

    static ResolutionBudget current()
    {
        return CURRENT.isBound() ? CURRENT.get() : new ResolutionBudget(DEFAULT_WORK_LIMIT);
    }

    /// An existing scope always wins: starting another solve cannot reset its allowance.
    public <T> T run(Supplier<T> action)
    {
        if (CURRENT.isBound()) {
            return action.get();
        }
        return ScopedValue.where(CURRENT, this).call(action::get);
    }

    public long work()
    {
        return work;
    }

    static void consume()
    {
        consume(1);
    }

    static void consume(long units)
    {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(USER_CANCELED, "Function type resolution was interrupted");
        }
        if (CURRENT.isBound()) {
            ResolutionBudget budget = CURRENT.get();
            if (units > budget.workLimit - budget.work) {
                budget.work = budget.workLimit;
                throw new TrinoException(TYPE_RESOLUTION_LIMIT_EXCEEDED, "Function type resolution exceeded the work limit of " + budget.workLimit);
            }
            budget.work += units;
        }
    }

    static void checkDepth(int depth)
    {
        consume();
        if (depth > MAX_NESTING) {
            throw new TrinoException(TYPE_RESOLUTION_LIMIT_EXCEEDED, "Function type resolution exceeded the nesting limit of " + MAX_NESTING);
        }
    }

    /// Guard recursive type/rule expansion separately from total work so a narrow but
    /// deeply nested input cannot overflow the Java stack before exhausting its allowance.
    static <T> T nested(Supplier<T> action)
    {
        return withBudget(() -> {
            ResolutionBudget budget = CURRENT.get();
            consume();
            if (budget.nesting >= MAX_NESTING) {
                throw new TrinoException(TYPE_RESOLUTION_LIMIT_EXCEEDED, "Function type resolution exceeded the nesting limit of " + MAX_NESTING);
            }
            budget.nesting++;
            try {
                return action.get();
            }
            finally {
                budget.nesting--;
            }
        });
    }
}
