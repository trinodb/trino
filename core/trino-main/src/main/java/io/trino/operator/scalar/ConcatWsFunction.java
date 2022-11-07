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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;

import java.lang.invoke.MethodHandle;
import java.util.Collections;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.addExact;

/**
 * Concatenate all but the first argument with separators. The first parameter is used as a separator.
 * The function needs at least 2 params.
 * <p>
 * Null behavior:
 * concat_ws(null, ...) returns null.
 * concat_ws(sep, arg1, arg2,..., argN) returns arg1 through argN concatenated with separator 'sep'.
 * Null arguments are ignored.
 * <p>
 * concat_ws(sep, array[strings]) returns elements in the array concatenated with separators.
 */
public final class ConcatWsFunction
        extends SqlScalarFunction
{
    public static final ConcatWsFunction CONCAT_WS = new ConcatWsFunction();
    private static final int MAX_INPUT_VALUES = 254;
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    @ScalarFunction("concat_ws")
    public static final class ConcatArrayWs
    {
        @SqlType("varchar")
        public static Slice concatWsArray(@SqlType("varchar") Slice separator, @SqlType("array(varchar)") Block elements)
        {
            return concatWs(
                    separator,
                    new SliceArray()
                    {
                        @Override
                        public Slice getElement(int i)
                        {
                            if (elements.isNull(i)) {
                                return null;
                            }
                            int sliceLength = elements.getSliceLength(i);
                            return elements.getSlice(i, 0, sliceLength);
                        }

                        @Override
                        public int getCount()
                        {
                            return elements.getPositionCount();
                        }
                    });
        }
    }

    public ConcatWsFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("concat_ws")
                        .returnType(VARCHAR)
                        .argumentType(VARCHAR)
                        .argumentType(VARCHAR)
                        .variableArity()
                        .build())
                .argumentNullability(false, true)
                .description("Concatenates elements using separator")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        int valueCount = boundSignature.getArity() - 1;
        if (valueCount < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments");
        }

        MethodHandle arrayMethodHandle = methodHandle(ConcatWsFunction.class, "concatWs", Slice.class, Slice[].class);
        MethodHandle customMethodHandle = arrayMethodHandle.asCollector(Slice[].class, valueCount);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.<InvocationConvention.InvocationArgumentConvention>builder()
                        .add(NEVER_NULL)
                        .addAll(Collections.nCopies(valueCount, BOXED_NULLABLE))
                        .build(),
                customMethodHandle);
    }

    @UsedByGeneratedCode
    public static Slice concatWs(Slice separator, Slice[] values)
    {
        return concatWs(
                separator,
                new SliceArray()
                {
                    @Override
                    public Slice getElement(int i)
                    {
                        return values[i];
                    }

                    @Override
                    public int getCount()
                    {
                        return values.length;
                    }
                });
    }

    private static Slice concatWs(Slice separator, SliceArray values)
    {
        if (values.getCount() > MAX_INPUT_VALUES) {
            throw new TrinoException(NOT_SUPPORTED, "Too many arguments for string concatenation");
        }

        // Validate size of output
        int length = 0;
        boolean requiresSeparator = false;

        for (int i = 0; i < values.getCount(); i++) {
            Slice value = values.getElement(i);

            if (value == null) {
                continue;
            }

            if (requiresSeparator) {
                length = addExact(length, separator.length());
            }

            length = addExact(length, value.length());
            requiresSeparator = true;

            if (length > MAX_OUTPUT_LENGTH) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
            }
        }

        // Build output
        Slice result = Slices.allocate(length);
        int position = 0;
        requiresSeparator = false;

        for (int i = 0; i < values.getCount(); i++) {
            Slice value = values.getElement(i);

            if (value == null) {
                continue;
            }

            if (requiresSeparator) {
                result.setBytes(position, separator);
                position += separator.length();
            }
            result.setBytes(position, value);
            position += value.length();
            requiresSeparator = true;
        }

        return result;
    }

    private interface SliceArray
    {
        Slice getElement(int i);

        int getCount();
    }
}
