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
package io.trino.operator.scalar;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.rowType;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;

public final class ZipFunction
        extends SqlScalarFunction
{
    public static final int MIN_ARITY = 2;
    public static final int MAX_ARITY = 5;
    public static final ZipFunction[] ZIP_FUNCTIONS;

    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipFunction.class, "zip", List.class, Block[].class);

    static {
        ZIP_FUNCTIONS = new ZipFunction[MAX_ARITY - MIN_ARITY + 1];
        for (int arity = MIN_ARITY; arity <= MAX_ARITY; arity++) {
            ZIP_FUNCTIONS[arity - MIN_ARITY] = new ZipFunction(arity);
        }
    }

    private ZipFunction(int arity)
    {
        this(IntStream.rangeClosed(1, arity).mapToObj(s -> "T" + s).collect(toImmutableList()));
    }

    private ZipFunction(List<String> typeParameters)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("zip")
                        .typeVariableConstraints(typeParameters.stream().map(TypeVariableConstraint::typeVariable).collect(toImmutableList()))
                        .returnType(arrayType(rowType(typeParameters.stream()
                                .map(TypeSignature::new)
                                .map(TypeSignatureParameter::anonymousField)
                                .collect(toImmutableList()))))
                        .argumentTypes(typeParameters.stream()
                                .map(name -> arrayType(new TypeSignature(name)))
                                .collect(toImmutableList()))
                        .build())
                .description("Merges the given arrays, element-wise, into a single array of rows.")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        List<Type> types = boundSignature.getArgumentTypes().stream()
                .map(ArrayType.class::cast)
                .map(ArrayType::getElementType)
                .collect(toImmutableList());
        List<Class<?>> javaArgumentTypes = nCopies(types.size(), Block.class);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(types).asVarargsCollector(Block[].class).asType(methodType(Block.class, javaArgumentTypes));
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(types.size(), NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block zip(List<Type> types, Block... arrays)
    {
        int biggestCardinality = 0;
        for (Block array : arrays) {
            biggestCardinality = Math.max(biggestCardinality, array.getPositionCount());
        }
        RowType rowType = RowType.anonymous(types);
        RowBlockBuilder outputBuilder = rowType.createBlockBuilder(null, biggestCardinality);
        for (int outputPosition = 0; outputPosition < biggestCardinality; outputPosition++) {
            buildRow(types, outputBuilder, outputPosition, arrays);
        }
        return outputBuilder.build();
    }

    private static void buildRow(List<Type> types, RowBlockBuilder outputBuilder, int outputPosition, Block[] arrays)
    {
        outputBuilder.buildEntry(fieldBuilders -> {
            for (int fieldIndex = 0; fieldIndex < arrays.length; fieldIndex++) {
                if (arrays[fieldIndex].getPositionCount() <= outputPosition) {
                    fieldBuilders.get(fieldIndex).appendNull();
                }
                else {
                    types.get(fieldIndex).appendTo(arrays[fieldIndex], outputPosition, fieldBuilders.get(fieldIndex));
                }
            }
        });
    }
}
