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
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static java.util.Objects.requireNonNull;

public class OrCodeGenerator
        implements BytecodeGenerator
{
    private final List<RowExpression> terms;

    public OrCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");
        checkArgument(specialForm.arguments().size() >= 2);

        terms = specialForm.arguments();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator)
    {
        Variable wasNull = generator.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("OR")
                .setDescription("OR");

        block.push(false); // keep track of whether we've seen a null so far

        LabelNode end = new LabelNode("end");
        LabelNode returnTrue = new LabelNode("returnTrue");
        for (int i = 0; i < terms.size(); i++) {
            RowExpression term = terms.get(i);
            block.append(generator.generate(term));

            IfStatement ifWasNull = new IfStatement("if term " + i + " wasNull...")
                    .condition(wasNull);

            ifWasNull.ifTrue()
                    .comment("clear the null flag, pop residual value off stack, and push was null flag on the stack (true)")
                    .pop(term.type().getJavaType()) // discard residual value
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
