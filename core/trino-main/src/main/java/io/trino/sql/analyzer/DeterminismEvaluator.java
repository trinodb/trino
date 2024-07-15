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
package io.trino.sql.analyzer;

import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Determines whether a given Expression is deterministic
 */
public final class DeterminismEvaluator
{
    private DeterminismEvaluator() {}

    public static boolean isDeterministic(Expression expression, Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier)
    {
        requireNonNull(resolvedFunctionSupplier, "resolvedFunctionSupplier is null");
        requireNonNull(expression, "expression is null");

        AtomicBoolean deterministic = new AtomicBoolean(true);
        new Visitor(resolvedFunctionSupplier).process(expression, deterministic);
        return deterministic.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<AtomicBoolean>
    {
        private final Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier;

        public Visitor(Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier)
        {
            this.resolvedFunctionSupplier = resolvedFunctionSupplier;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean deterministic)
        {
            if (!resolvedFunctionSupplier.apply(node).deterministic()) {
                deterministic.set(false);
                return null;
            }
            return super.visitFunctionCall(node, deterministic);
        }
    }

    public static boolean containsCurrentTimeFunctions(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean hasTemporalFunction = new AtomicBoolean(false);
        new DeterminismEvaluator.TemporalFunctionVisitor().process(expression, hasTemporalFunction);
        return hasTemporalFunction.get();
    }

    private static class TemporalFunctionVisitor
            extends DefaultExpressionTraversalVisitor<AtomicBoolean>
    {
        @Override
        protected Void visitCurrentDate(CurrentDate node, AtomicBoolean currentTime)
        {
            currentTime.set(true);
            return null;
        }

        @Override
        protected Void visitCurrentTime(CurrentTime node, AtomicBoolean currentTime)
        {
            currentTime.set(true);
            return null;
        }

        @Override
        protected Void visitCurrentTimestamp(CurrentTimestamp node, AtomicBoolean currentTime)
        {
            currentTime.set(true);
            return null;
        }

        @Override
        protected Void visitLocalTime(LocalTime node, AtomicBoolean currentTime)
        {
            currentTime.set(true);
            return null;
        }

        @Override
        protected Void visitLocalTimestamp(LocalTimestamp node, AtomicBoolean currentTime)
        {
            currentTime.set(true);
            return null;
        }
    }
}
