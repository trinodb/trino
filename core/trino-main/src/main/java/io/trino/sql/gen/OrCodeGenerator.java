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

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;

import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.trino.sql.gen.ConditionalPredication.canPredicate;
import static java.util.Objects.requireNonNull;

public class OrCodeGenerator
        implements BytecodeGenerator
{
    private final List<Expression> terms;

    public OrCodeGenerator(Logical logical)
    {
        requireNonNull(logical, "logical is null");
        terms = logical.terms();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator)
    {
        if (canPredicate(generator, terms)) {
            return generatePredicated(generator);
        }
        return generateShortCircuit(generator);
    }

    /**
     * Branchless, three-valued OR for infallible, deterministic terms: evaluate every term unconditionally
     * and fold the results with boolean algebra, instead of short-circuiting with a data-dependent branch.
     * All values stay primitive ({@code boolean} value channel + {@code wasNull} flag), so no boxing is
     * introduced.
     */
    private BytecodeNode generatePredicated(BytecodeGeneratorContext generator)
    {
        Scope scope = generator.getScope();
        Variable wasNull = generator.wasNull();
        Variable sawNull = scope.createTempVariable(boolean.class);
        Variable sawTrue = scope.createTempVariable(boolean.class);
        Variable value = scope.createTempVariable(boolean.class);

        BytecodeBlock block = new BytecodeBlock()
                .comment("OR (predicated)")
                .setDescription("OR (predicated)");

        block.append(sawNull.set(constantFalse()));
        block.append(sawTrue.set(constantFalse()));

        for (Expression term : terms) {
            block.append(wasNull.set(constantFalse()));
            block.append(generator.generate(term)); // leaves the boolean value on the stack, sets wasNull
            block.putVariable(value);
            block.append(sawNull.set(or(sawNull, wasNull)));
            block.append(sawTrue.set(or(sawTrue, and(not(wasNull), value))));
        }

        // NULL only when no term was true but some term was null; otherwise the value below is exact
        block.append(wasNull.set(and(sawNull, not(sawTrue))));
        // result is TRUE when any term was true (value is ignored when wasNull is set)
        block.append(sawTrue);

        return block;
    }

    private BytecodeNode generateShortCircuit(BytecodeGeneratorContext generator)
    {
        Variable wasNull = generator.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("OR")
                .setDescription("OR");

        block.push(false); // keep track of whether we've seen a null so far

        LabelNode end = new LabelNode("end");
        LabelNode returnTrue = new LabelNode("returnTrue");
        for (int i = 0; i < terms.size(); i++) {
            Expression term = terms.get(i);
            block.append(generator.generate(term));

            IfStatement ifWasNull = new IfStatement("if term " + i + " wasNull...")
                    .condition(wasNull);

            ifWasNull.ifTrue()
                    .comment("clear the null flag, pop residual value off stack, and push was null flag on the stack (true)")
                    .pop(generator.getCallSiteBinder().getAccessibleType(term.type().getJavaType())) // discard residual value
                    .pop(boolean.class) // discard the previous "we've seen a null flag"
                    .push(true);

            ifWasNull.ifFalse()
                    .comment("if term is true, short circuit and return true")
                    .ifTrueGoto(returnTrue);

            block.append(ifWasNull)
                    .append(wasNull.set(constantFalse())); // prepare for the next loop
        }

        block.putVariable(wasNull)
                .push(false) // result is false
                .gotoLabel(end);

        block.visitLabel(returnTrue)
                .append(wasNull.set(constantFalse()))
                .pop(boolean.class) // discard the previous "we've seen a null flag"
                .push(true); // result is true

        block.visitLabel(end);

        return block;
    }
}
