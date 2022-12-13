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

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.TryExpression;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Determines whether a given Expression is safe (aka. exception-free)
 */
public final class SafeExpressionEvaluator
{
    private SafeExpressionEvaluator() {}

    public static boolean isSafeExpression(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(true);
        new Visitor().process(expression, result);
        return result.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<AtomicBoolean>
    {
        public Visitor() {}

        @Override
        protected Void visitCast(Cast node, AtomicBoolean result)
        {
            result.set(false);
            return null;
        }

        @Override
        protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, AtomicBoolean result)
        {
            // May cause overflow, underflow or division by zero
            result.set(false);
            return null;
        }

        @Override
        protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, AtomicBoolean result)
        {
            result.set(false);
            return null;
        }

        @Override
        protected Void visitTryExpression(TryExpression node, AtomicBoolean result)
        {
            result.set(false);
            return null;
        }

        @Override
        protected Void visitLambdaExpression(LambdaExpression node, AtomicBoolean result)
        {
            result.set(false);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean result)
        {
            // By default all functions are not safe
            result.set(false);
            return null;
        }
    }
}
