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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.BytecodeVisitor;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.trino.spi.type.Type;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;

/**
 * Generates bytecode to read a value from a Block at a given position.
 */
final class InputReferenceCompiler
{
    private InputReferenceCompiler() {}

    public static BytecodeNode generateInputReference(CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
    {
        return new InputReferenceNode(callSiteBinder, scope, type, block, position);
    }

    static class InputReferenceNode
            implements BytecodeNode
    {
        private final BytecodeNode body;
        private final BytecodeExpression block;
        private final BytecodeExpression position;

        private InputReferenceNode(CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
        {
            // Generate body based on block and position
            Variable wasNullVariable = scope.getVariable("wasNull");
            Class<?> callType = type.getJavaType();
            if (!callType.isPrimitive() && callType != Slice.class) {
                callType = Object.class;
            }

            IfStatement ifStatement = new IfStatement();
            ifStatement.condition(block.invoke("isNull", boolean.class, position));

            ifStatement.ifTrue()
                    .putVariable(wasNullVariable, true)
                    .pushJavaDefault(callType);

            String methodName = "get" + Primitives.wrap(callType).getSimpleName();
            BytecodeExpression value = constantType(callSiteBinder, type).invoke(methodName, callType, block, position);
            Class<?> expectedType = callSiteBinder.getAccessibleType(type.getJavaType());
            if (callType != expectedType) {
                value = value.cast(expectedType);
            }
            ifStatement.ifFalse(value);

            this.body = ifStatement;
            this.block = block;
            this.position = position;
        }

        @Override
        public List<BytecodeNode> getChildNodes()
        {
            return ImmutableList.of();
        }

        @Override
        public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
        {
            body.accept(visitor, generationContext);
        }

        @Override
        public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
        {
            return visitor.visitIf(parent, (IfStatement) body);
        }

        public BytecodeNode produceBlockAndPosition()
        {
            BytecodeBlock blockAndPosition = new BytecodeBlock();
            blockAndPosition.append(block);
            blockAndPosition.append(position);
            return blockAndPosition;
        }

        public BytecodeExpression blockAndPositionIsNull()
        {
            return block.invoke("isNull", boolean.class, position);
        }
    }
}
