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
package io.trino.sql.ir;

import io.trino.spi.type.Type;
import io.trino.sql.planner.SymbolAllocator;

import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;

/// Test helpers for building IR expressions whose construction needs a function resolver.
public final class TestingIr
{
    private TestingIr() {}

    /// Builds the canonical IR form of a comparison (a {@link Call} to the operator function),
    /// resolving the operator against the shared testing planner context. Comparison operators
    /// are builtin, so the resolved functions compare equal to those produced by any other
    /// metadata instance — expressions built here match those produced by production code under
    /// test.
    public static Expression comparison(ComparisonOperator operator, Expression left, Expression right)
    {
        return IrExpressions.comparison(PLANNER_CONTEXT.getMetadata(), operator, left, right);
    }

    /// Builds the desugared IR form of a `value BETWEEN min AND max` predicate (see
    /// {@link IrExpressions#between}), resolving comparison operators against the shared testing
    /// planner context.
    public static Expression between(Expression value, Expression min, Expression max)
    {
        return IrExpressions.between(PLANNER_CONTEXT.getMetadata(), new SymbolAllocator(), value, min, max);
    }

    /// Builds the desugared IR form of `NULLIF(first, second)` (see {@link IrExpressions#nullIf}),
    /// resolving the equality operator against the shared testing planner context.
    public static Expression nullIf(SymbolAllocator allocator, Expression first, Expression second)
    {
        return IrExpressions.nullIf(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), allocator, first, second);
    }

    public static Expression nullIf(SymbolAllocator allocator, Expression first, Expression second, Type comparisonType)
    {
        return IrExpressions.nullIf(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), allocator, first, second, comparisonType);
    }
}
