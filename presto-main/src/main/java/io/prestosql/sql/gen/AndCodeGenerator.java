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
package io.prestosql.sql.gen;

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static java.util.Objects.requireNonNull;

public class AndCodeGenerator
        implements BytecodeGenerator
{
    private final RowExpression left;
    private final RowExpression right;

    public AndCodeGenerator(SpecialForm specialForm)
    {
        requireNonNull(specialForm, "specialForm is null");

        checkArgument(specialForm.getArguments().size() == 2);
        left = specialForm.getArguments().get(0);
        right = specialForm.getArguments().get(1);
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator)
    {
        Variable wasNull = generator.wasNull();
        BytecodeBlock block = new BytecodeBlock()
                .comment("AND")
                .setDescription("AND");

        block.append(generator.generate(left));

        IfStatement ifLeftIsNull = new IfStatement("if left wasNull...")
                .condition(wasNull);

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue()
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .append(wasNull.set(constantFalse()))
                .pop(left.getType().getJavaType()) // discard left value
                .push(true);

        LabelNode leftIsTrue = new LabelNode("leftIsTrue");
        ifLeftIsNull.ifFalse()
                .comment("if left is false, push false, and goto end")
                .ifTrueGoto(leftIsTrue)
                .push(false)
                .gotoLabel(end)
                .comment("left was true; push left null flag on the stack (false)")
                .visitLabel(leftIsTrue)
                .push(false);

        block.append(ifLeftIsNull);

        // At this point we know the left expression was either NULL or TRUE.  The stack contains a single boolean
        // value for this expression which indicates if the left value was NULL.

        // eval right!
        block.append(generator.generate(right));

        IfStatement ifRightIsNull = new IfStatement("if right wasNull...");
        ifRightIsNull.condition()
                .append(wasNull);

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue()
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(right.getType().getJavaType());

        LabelNode rightIsTrue = new LabelNode("rightIsTrue");
        ifRightIsNull.ifFalse()
                .comment("if right is false, pop left null flag off stack, push false and goto end")
                .ifTrueGoto(rightIsTrue)
                .pop(boolean.class)
                .push(false)
                .gotoLabel(end)
                .comment("right was true; store left null flag (on stack) in wasNull variable, and push true")
                .visitLabel(rightIsTrue)
                .putVariable(wasNull)
                .push(true);

        block.append(ifRightIsNull)
                .visitLabel(end);

        return block;
    }
}
