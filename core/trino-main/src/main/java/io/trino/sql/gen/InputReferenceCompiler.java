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
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.BytecodeVisitor;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.objectweb.asm.MethodVisitor;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;

/**
 * Generates bytecode to read a value from a Block at a given position.
 */
final class InputReferenceCompiler
{
    private InputReferenceCompiler() {}

    public static BytecodeNode generateInputReference(TypeOperators typeOperators, CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
    {
        return new InputReferenceNode(typeOperators, callSiteBinder, scope, type, block, position);
    }

    static class InputReferenceNode
            implements BytecodeNode
    {
        private final BytecodeBlock body;
        private final Variable inputBlock;
        private final BytecodeExpression block;
        private final BytecodeExpression position;
        private final Variable valueBlock;
        private final Variable valuePosition;

        private InputReferenceNode(TypeOperators typeOperators, CallSiteBinder callSiteBinder, Scope scope, Type type, BytecodeExpression block, BytecodeExpression position)
        {
            // Generate body based on block and position
            Variable wasNullVariable = scope.getVariable("wasNull");
            Class<?> callType = type.getJavaType();
            if (!callType.isPrimitive() && callType != Slice.class) {
                callType = Object.class;
            }

            Variable inputBlock = scope.createTempVariable(Block.class);
            Variable valueBlock = scope.createTempVariable(ValueBlock.class);
            Variable valuePosition = scope.createTempVariable(int.class);

            IfStatement ifStatement = new IfStatement();
            ifStatement.condition(valueBlock.invoke("isNull", boolean.class, valuePosition));

            ifStatement.ifTrue()
                    .putVariable(wasNullVariable, true)
                    .pushJavaDefault(callType);

            BytecodeExpression value;
            if (callType == Object.class) {
                value = constantType(callSiteBinder, type).invoke("getObject", Object.class, valueBlock.cast(Block.class), valuePosition);
            }
            else {
                MethodHandle readValue = typeOperators.getReadValueOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
                readValue = readValue.asType(readValue.type().changeReturnType(callType));
                value = invoke(callSiteBinder.bind(readValue), "readValue", valueBlock, valuePosition);
            }
            Class<?> expectedType = callSiteBinder.getAccessibleType(type.getJavaType());
            if (callType != expectedType) {
                value = value.cast(expectedType);
            }
            ifStatement.ifFalse(value);

            this.inputBlock = inputBlock;
            this.block = block;
            this.position = position;
            this.valueBlock = valueBlock;
            this.valuePosition = valuePosition;
            this.body = loadValueBlockAndPosition()
                    .append(ifStatement);
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
            return visitor.visitBlock(parent, body);
        }

        public BytecodeNode produceValueBlockAndPosition()
        {
            return loadValueBlockAndPosition()
                    .append(valueBlock)
                    .append(valuePosition);
        }

        private BytecodeBlock loadValueBlockAndPosition()
        {
            return new BytecodeBlock()
                    .append(inputBlock.set(block))
                    .append(valueBlock.set(inputBlock.invoke("getUnderlyingValueBlock", ValueBlock.class)))
                    .append(valuePosition.set(inputBlock.invoke("getUnderlyingValuePosition", int.class, position)));
        }

        public BytecodeExpression valueBlockPositionIsNull()
        {
            return valueBlock.invoke("isNull", boolean.class, valuePosition);
        }
    }
}
