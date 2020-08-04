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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;

import java.lang.invoke.MethodHandle;
import java.util.Collections;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.addExact;

/**
 * Concatenate all but the first argument with separators. The first parameter is used as a separator.
 * The function needs at least 2 params.
 *
 * Null behavior:
 * concat_ws(null, ...) returns null.
 * concat_ws(sep, arg1, arg2,..., argN) returns arg1 through argN concatenated with separator 'sep'.
 * Null arguments are ignored.
 *
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
                            else {
                                int sliceLength = elements.getSliceLength(i);
                                return elements.getSlice(i, 0, sliceLength);
                            }
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
        super(new FunctionMetadata(
                new Signature(
                        "concat_ws",
                        ImmutableList.of(),
                        ImmutableList.of(),
                        VARCHAR.getTypeSignature(),
                        ImmutableList.of(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                        true),
                true,
                ImmutableList.of(new FunctionArgumentDefinition(false), new FunctionArgumentDefinition(true)),
                false,
                true,
                "Concatenates elements using separator",
                SCALAR));
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding binding)
    {
        int valueCount = binding.getArity() - 1;
        if (valueCount < 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments");
        }

        MethodHandle arrayMethodHandle = methodHandle(ConcatWsFunction.class, "concatWs", Slice.class, Slice[].class);
        MethodHandle customMethodHandle = arrayMethodHandle.asCollector(Slice[].class, valueCount);

        return new ChoicesScalarFunctionImplementation(
                binding,
                NULLABLE_RETURN,
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
                new SliceArray() {
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
            throw new PrestoException(NOT_SUPPORTED, "Too many arguments for string concatenation");
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
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
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
