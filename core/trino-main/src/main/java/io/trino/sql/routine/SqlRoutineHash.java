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
package io.trino.sql.routine;

import com.google.common.hash.Hasher;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.Type;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.routine.ir.IrBlock;
import io.trino.sql.routine.ir.IrBreak;
import io.trino.sql.routine.ir.IrContinue;
import io.trino.sql.routine.ir.IrIf;
import io.trino.sql.routine.ir.IrLabel;
import io.trino.sql.routine.ir.IrLoop;
import io.trino.sql.routine.ir.IrNodeVisitor;
import io.trino.sql.routine.ir.IrRepeat;
import io.trino.sql.routine.ir.IrReturn;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.routine.ir.IrSet;
import io.trino.sql.routine.ir.IrStatement;
import io.trino.sql.routine.ir.IrVariable;
import io.trino.sql.routine.ir.IrWhile;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class SqlRoutineHash
{
    private SqlRoutineHash() {}

    public static void hash(IrRoutine routine, Hasher hasher, BlockEncodingSerde blockEncodingSerde)
    {
        routine.accept(new HashRoutineIrVisitor(hasher, blockEncodingSerde), null);
    }

    private record HashRoutineIrVisitor(Hasher hasher, BlockEncodingSerde blockEncodingSerde)
            implements IrNodeVisitor<Void, Void>, RowExpressionVisitor<Void, Void>
    {
        @Override
        public Void visitRoutine(IrRoutine node, Void context)
        {
            hashClassName(node.getClass());
            hashType(node.returnType());

            hasher.putInt(node.parameters().size());
            for (IrVariable parameter : node.parameters()) {
                process(parameter, context);
            }
            process(node.body(), context);
            return null;
        }

        @Override
        public Void visitVariable(IrVariable node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putInt(node.field());
            hashType(node.type());

            visitRowExpression(node.defaultValue());
            return null;
        }

        @Override
        public Void visitBlock(IrBlock node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putBoolean(node.label().isPresent());
            node.label().ifPresent(this::hashLabel);

            hasher.putInt(node.variables().size());
            for (IrVariable variable : node.variables()) {
                process(variable, context);
            }
            hasher.putInt(node.statements().size());
            for (IrStatement statement : node.statements()) {
                process(statement, context);
            }
            return null;
        }

        @Override
        public Void visitBreak(IrBreak node, Void context)
        {
            hashClassName(node.getClass());
            hashLabel(node.target());
            return null;
        }

        @Override
        public Void visitContinue(IrContinue node, Void context)
        {
            hashClassName(node.getClass());
            hashLabel(node.target());
            return null;
        }

        @Override
        public Void visitIf(IrIf node, Void context)
        {
            hashClassName(node.getClass());

            visitRowExpression(node.condition());
            process(node.ifTrue(), context);

            hasher.putBoolean(node.ifFalse().isPresent());
            if (node.ifFalse().isPresent()) {
                process(node.ifFalse().get(), context);
            }
            return null;
        }

        @Override
        public Void visitWhile(IrWhile node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putBoolean(node.label().isPresent());
            node.label().ifPresent(this::hashLabel);

            visitRowExpression(node.condition());
            process(node.body(), context);
            return null;
        }

        @Override
        public Void visitRepeat(IrRepeat node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putBoolean(node.label().isPresent());
            node.label().ifPresent(this::hashLabel);

            visitRowExpression(node.condition());
            process(node.block(), context);
            return null;
        }

        @Override
        public Void visitLoop(IrLoop node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putBoolean(node.label().isPresent());
            node.label().ifPresent(this::hashLabel);

            process(node.block(), context);
            return null;
        }

        @Override
        public Void visitReturn(IrReturn node, Void context)
        {
            hashClassName(node.getClass());
            visitRowExpression(node.value());
            return null;
        }

        @Override
        public Void visitSet(IrSet node, Void context)
        {
            hashClassName(node.getClass());

            visitRowExpression(node.value());
            process(node.target(), context);
            return null;
        }

        public void visitRowExpression(RowExpression expression)
        {
            expression.accept(this, null);
        }

        @Override
        public Void visitCall(CallExpression call, Void context)
        {
            hashClassName(call.getClass());
            hashResolvedFunction(call.resolvedFunction());
            hasher.putInt(call.arguments().size());
            call.arguments().forEach(this::visitRowExpression);
            return null;
        }

        @Override
        public Void visitInputReference(InputReferenceExpression reference, Void context)
        {
            hashClassName(reference.getClass());
            hasher.putInt(reference.field());
            hashType(reference.type());
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, Void context)
        {
            hashClassName(literal.getClass());
            hashType(literal.type());

            Object value = literal.value();
            hasher.putBoolean(value == null);

            switch (value) {
                case null -> {}
                case Boolean booleanValue -> hasher.putBoolean(booleanValue);
                case Byte byteValue -> hasher.putByte(byteValue);
                case Short shortValue -> hasher.putShort(shortValue);
                case Integer intValue -> hasher.putInt(intValue);
                case Long longValue -> hasher.putLong(longValue);
                case Float floatValue -> hasher.putFloat(floatValue);
                case Double doubleValue -> hasher.putDouble(doubleValue);
                case byte[] byteArrayValue -> hasher.putBytes(byteArrayValue);
                case String stringValue -> hasher.putString(stringValue, UTF_8);
                case Slice sliceValue -> hasher.putBytes(sliceValue.getBytes());
                default -> {
                    Block block = literal.getBlockValue();
                    SliceOutput output = new DynamicSliceOutput(toIntExact(block.getSizeInBytes() + block.getEncodingName().length() + (2 * Integer.BYTES)));
                    blockEncodingSerde.writeBlock(output, block);
                    hasher.putBytes(output.slice().getBytes());
                }
            }

            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            hashClassName(lambda.getClass());

            hasher.putInt(lambda.arguments().size());
            lambda.arguments().forEach(symbol -> {
                hashString(symbol.name());
                hashType(symbol.type());
            });

            visitRowExpression(lambda.body());
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            hashClassName(reference.getClass());
            hashString(reference.name());
            hashType(reference.type());
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, Void context)
        {
            hashClassName(specialForm.getClass());

            hashType(specialForm.type());
            hasher.putInt(specialForm.form().ordinal());

            hasher.putInt(specialForm.arguments().size());
            specialForm.arguments().forEach(this::visitRowExpression);

            hasher.putInt(specialForm.functionDependencies().size());
            specialForm.functionDependencies().forEach(this::hashResolvedFunction);

            return null;
        }

        private void hashClassName(Class<?> clazz)
        {
            hashString(clazz.getName());
        }

        private void hashType(Type type)
        {
            hashString(type.toString());
        }

        private void hashLabel(IrLabel label)
        {
            hashString(label.name());
        }

        private void hashResolvedFunction(ResolvedFunction function)
        {
            BoundSignature signature = function.signature();
            hashString(signature.getName().toString());
            hashType(signature.getReturnType());
            hasher.putInt(signature.getArgumentTypes().size());
            signature.getArgumentTypes().forEach(this::hashType);

            hashString(function.catalogHandle().getId());
            hashString(function.functionId().toString());

            hasher.putInt(function.typeDependencies().size());
            function.typeDependencies().forEach((typeSignature, type) -> {
                hashString(typeSignature.toString());
                hashType(type);
            });

            hasher.putInt(function.functionDependencies().size());
            function.functionDependencies().forEach(this::hashResolvedFunction);
        }

        private void hashString(String string)
        {
            hasher.putString(string, UTF_8);
        }
    }
}
