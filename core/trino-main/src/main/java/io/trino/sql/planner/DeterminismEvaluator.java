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

import io.trino.sql.ir.Call;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Determines whether a given Expression is deterministic
 */
public final class DeterminismEvaluator
{
    private DeterminismEvaluator() {}

    public static boolean isDeterministic(Expression expression)
    {
        AtomicBoolean deterministic = new AtomicBoolean(true);
        new Visitor().process(expression, deterministic);
        return deterministic.get();
    }

    private static class Visitor
            extends DefaultTraversalVisitor<AtomicBoolean>
    {
        @Override
        protected Void visitCall(Call node, AtomicBoolean deterministic)
        {
            if (!node.function().deterministic()) {
                deterministic.set(false);
                return null;
            }
            return super.visitCall(node, deterministic);
        }
    }
}
