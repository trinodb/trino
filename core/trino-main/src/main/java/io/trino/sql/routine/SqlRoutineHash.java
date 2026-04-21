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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
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
            implements IrNodeVisitor<Void, Void>
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

            hashExpression(node.defaultValue());
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

            hashExpression(node.condition());
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

            hashExpression(node.condition());
            process(node.body(), context);
            return null;
        }

        @Override
        public Void visitRepeat(IrRepeat node, Void context)
        {
            hashClassName(node.getClass());
            hasher.putBoolean(node.label().isPresent());
            node.label().ifPresent(this::hashLabel);

            hashExpression(node.condition());
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
            hashExpression(node.value());
            return null;
        }

        @Override
        public Void visitSet(IrSet node, Void context)
        {
            hashClassName(node.getClass());

            hashExpression(node.value());
            process(node.target(), context);
            return null;
        }

        private void hashExpression(Expression expression)
        {
            hashClassName(expression.getClass());
            hashType(expression.type());

            switch (expression) {
                case Call call -> {
                    hashResolvedFunction(call.function());
                    hasher.putInt(call.arguments().size());
                    call.arguments().forEach(this::hashExpression);
                }
                case Constant constant -> {
                    Object value = constant.value();
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
                            // For complex types (e.g., Block-backed arrays/rows/maps), serialize canonically
                            BlockBuilder blockBuilder = constant.type().createBlockBuilder(null, 1);
                            TypeUtils.writeNativeValue(constant.type(), blockBuilder, value);
                            Block block = blockBuilder.build();
                            SliceOutput output = new DynamicSliceOutput(toIntExact(blockEncodingSerde.estimatedWriteSize(block)));
                            blockEncodingSerde.writeBlock(output, block);
                            hasher.putBytes(output.slice().getBytes());
                        }
                    }
                }
                case Comparison comparison -> {
                    hashString(comparison.operator().name());
                    hashExpression(comparison.left());
                    hashExpression(comparison.right());
                }
                case Logical logical -> {
                    hashString(logical.operator().name());
                    hasher.putInt(logical.terms().size());
                    logical.terms().forEach(this::hashExpression);
                }
                case Reference reference -> hashString(reference.name());
                case Lambda lambda -> {
                    hasher.putInt(lambda.arguments().size());
                    lambda.arguments().forEach(symbol -> {
                        hashString(symbol.name());
                        hashType(symbol.type());
                    });
                    hashExpression(lambda.body());
                }
                case FieldReference fieldRef -> {
                    hasher.putInt(fieldRef.field());
                    hasher.putInt(expression.children().size());
                    for (Expression child : expression.children()) {
                        hashExpression(child);
                    }
                }
                // These expression types are fully represented by class name + type + children
                case Array _, Between _, Bind _, Case _, Cast _, Coalesce _,
                     In _, IsNull _, NullIf _, Row _, Switch _ -> {
                    hasher.putInt(expression.children().size());
                    for (Expression child : expression.children()) {
                        hashExpression(child);
                    }
                }
            }
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
