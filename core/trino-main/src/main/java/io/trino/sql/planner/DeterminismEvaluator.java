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
package io.trino.sql.planner;

import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Determines whether a given Expression is deterministic
 */
public final class DeterminismEvaluator
{
    private DeterminismEvaluator() {}

    public static boolean isDeterministic(Expression expression, Metadata metadata)
    {
        return isDeterministic(expression, functionCall -> metadata.decodeFunction(functionCall.getName()));
    }

    public static boolean isDeterministic(Expression expression, Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier)
    {
        requireNonNull(resolvedFunctionSupplier, "resolvedFunctionSupplier is null");
        requireNonNull(expression, "expression is null");

        AtomicBoolean deterministic = new AtomicBoolean(true);
        new Visitor(resolvedFunctionSupplier).process(expression, deterministic);
        return deterministic.get();
    }

    private static class Visitor
            extends DefaultTraversalVisitor<AtomicBoolean>
    {
        private final Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier;

        public Visitor(Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier)
        {
            this.resolvedFunctionSupplier = resolvedFunctionSupplier;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean deterministic)
        {
            if (!resolvedFunctionSupplier.apply(node).isDeterministic()) {
                deterministic.set(false);
                return null;
            }
            return super.visitFunctionCall(node, deterministic);
        }
    }
}
