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
package io.trino.sql.gen;

import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;

/**
 * Generates code that evaluates an expression of a particular shape and writes the result straight into a
 * block builder, without materializing an intermediate value (for example a {@code SqlRow}) that would
 * be copied into the output afterwards.
 * <p>
 * A writer is an optimization of the generic path (evaluate the expression, then write the value), so it
 * must produce exactly the same output, nulls included. The projection compiler picks the first writer
 * whose {@link #supports} accepts the expression, generates it once and falls back to the generic path
 * only when the result does not fit a JIT compilable method. Writers are therefore matched on disjoint
 * shapes and never try several alternatives for one expression.
 * <p>
 * The {@code wasNull} flag is cleared before a writer runs and is undefined when it finishes.
 * Anything a writer evaluates besides the expression itself, such as a nested value, must go through
 * {@link Context#write}, which dispatches to the writers again.
 */
public interface ProjectionWriter
{
    /**
     * Cheap structural check, no code is generated.
     */
    boolean supports(Expression expression, Type type);

    /**
     * Appends the value of {@code expression} (of destination type {@code type}) to {@code output}, a
     * {@code BlockBuilder} expression that may be evaluated more than once.
     */
    BytecodeNode write(Context context, Expression expression, Type type, BytecodeExpression output);

    interface Context
    {
        Scope scope();

        BytecodeGeneratorContext generatorContext();

        /**
         * Writes a nested expression to {@code output}.
         */
        BytecodeNode write(Expression expression, Type type, BytecodeExpression output);
    }
}
