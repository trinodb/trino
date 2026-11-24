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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SubqueryExpression;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.trino.sql.tree.AstTraversal.forEachChild;
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
        evaluate(expression, resolvedFunctionSupplier, deterministic);
        return deterministic.get();
    }

    private static void evaluate(Node node, Function<FunctionCall, ResolvedFunction> resolvedFunctionSupplier, AtomicBoolean deterministic)
    {
        switch (node) {
            case SubqueryExpression _ -> {
                // Don't traverse into subqueries within an expression
            }
            case FunctionCall functionCall -> {
                if (!resolvedFunctionSupplier.apply(functionCall).deterministic()) {
                    deterministic.set(false);
                    return;
                }
                forEachChild(node, child -> evaluate(child, resolvedFunctionSupplier, deterministic));
            }
            default -> forEachChild(node, child -> evaluate(child, resolvedFunctionSupplier, deterministic));
        }
    }

    public static boolean containsCurrentTimeFunctions(Node node)
    {
        requireNonNull(node, "node is null");

        return hasTemporalFunction(node);
    }

    private static boolean hasTemporalFunction(Node node)
    {
        return switch (node) {
            case CurrentDate _, CurrentTime _, CurrentTimestamp _, LocalTime _, LocalTimestamp _ -> true;
            // Don't traverse into subqueries within an expression
            case SubqueryExpression _ -> false;
            default -> {
                AtomicBoolean found = new AtomicBoolean(false);
                forEachChild(node, child -> found.compareAndSet(false, hasTemporalFunction(child)));
                yield found.get();
            }
        };
    }
}
