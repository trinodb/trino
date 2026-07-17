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
package io.trino.spi.connector;

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;

import java.util.Map;
import java.util.Set;

public interface ConnectorExpressionEvaluator
{
    /**
     * No-op evaluator whose {@link Prepared} always returns {@link EvaluationResult.NoResult},
     * indicating the expression cannot be evaluated.
     */
    ConnectorExpressionEvaluator NO_OP = (_, _) -> _ -> new EvaluationResult.NoResult();

    /**
     * Prepares to evaluate {@code expression} for a given {@code session}. Any one-time
     * compilation cost (e.g. deserializing an opaque engine expression) is paid here, once per
     * {@link io.trino.spi.connector.Constraint Constraint}. The returned {@link Prepared} handle
     * is then reused across multiple {@link Prepared#tryEvaluate} calls.
     */
    Prepared prepare(ConnectorSession session, ConnectorExpression expression);

    interface Prepared
    {
        /**
         * Evaluates the prepared expression against the given variable {@code bindings},
         * where each key is a variable name and each value is the current binding for
         * that variable.
         *
         * @return {@link EvaluationResult.Value} holding the result (which may be {@code null} for SQL NULL),
         *         or {@link EvaluationResult.NoResult} if the expression could not be evaluated.
         */
        EvaluationResult tryEvaluate(Map<String, NullableValue> bindings);

        /**
         * Returns the variable names that must be supplied in the {@code bindings} map for
         * {@link #tryEvaluate} to produce a definite result, or an empty set if the expression
         * references no column variables (in which case {@link #tryEvaluate} can be called with
         * an empty bindings map and will return either a definite result or {@link EvaluationResult.NoResult}).
         * Callers may use this to decide which columns need to be read before calling {@link #tryEvaluate}.
         */
        default Set<String> getArguments()
        {
            return Set.of();
        }
    }

    /**
     * The result of evaluating a prepared expression via {@link Prepared#tryEvaluate}.
     */
    sealed interface EvaluationResult
            permits EvaluationResult.Value, EvaluationResult.NoResult
    {
        /**
         * The expression evaluated successfully. {@code value} is {@code null} when the expression
         * evaluated to SQL NULL.
         */
        record Value(Object value)
                implements EvaluationResult {}

        /**
         * The expression could not be evaluated (e.g. unsupported expression structure, or
         * insufficient bindings).
         */
        record NoResult()
                implements EvaluationResult {}
    }
}
