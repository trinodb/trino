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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Switch;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public final class NullabilityAnalyzer
{
    private NullabilityAnalyzer() {}

    /**
     * TODO: this currently produces a very conservative estimate.
     * We need to narrow down the conditions under which certain constructs
     * can return null (e.g., if(a, b, c) might return null for non-null a
     * only if b or c can be null.
     */
    public static boolean mayReturnNullOnNonNullInput(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        new Visitor().process(expression, result);
        return result.get();
    }

    private static class Visitor
            extends DefaultTraversalVisitor<AtomicBoolean>
    {
        @Override
        protected Void visitCast(Cast node, AtomicBoolean result)
        {
            // Certain casts (e.g., cast(JSON 'null' AS ...)) can return null, but we know
            // that any "type-only" coercion cannot produce null on non-null input, so
            // take advantage of that fact to produce a more precise result.
            //
            // TODO: need a generic way to determine whether a CAST can produce null on non-null input.
            // This should be part of the metadata associated with the CAST. (N.B. the rules in
            // ISO/IEC 9075-2:2016, section 7.16.21 seems to imply that CAST cannot return NULL
            // except for the CAST(NULL AS x) case -- we should fix this at some point)
            //
            // Also, try_cast (i.e., safe cast) can return null
            process(node.expression(), result);
            result.compareAndSet(false, node.safe());
            return null;
        }

        @Override
        protected Void visitNullIf(NullIf node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitIn(In node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitCase(Case node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSwitch(Switch node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitFieldReference(FieldReference node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitCall(Call node, AtomicBoolean result)
        {
            if (node.function().functionNullability().isReturnNullable()) {
                result.set(true);
            }

            return null;
        }

        @Override
        protected Void visitConstant(Constant node, AtomicBoolean result)
        {
            if (node.value() == null) {
                result.set(true);
            }
            return null;
        }
    }
}
