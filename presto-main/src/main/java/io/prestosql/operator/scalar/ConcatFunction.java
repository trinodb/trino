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
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.addExact;
import static java.util.Collections.nCopies;

public final class ConcatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final ConcatFunction VARCHAR_CONCAT = new ConcatFunction(VARCHAR.getTypeSignature(), "Concatenates given strings");

    public static final ConcatFunction VARBINARY_CONCAT = new ConcatFunction(VARBINARY.getTypeSignature(), "concatenates given varbinary values");

    private static final int MAX_INPUT_VALUES = 254;
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private ConcatFunction(TypeSignature type, String description)
    {
        super(new FunctionMetadata(
                new Signature(
                        "concat",
                        ImmutableList.of(),
                        ImmutableList.of(),
                        type,
                        ImmutableList.of(type),
                        true),
                false,
                ImmutableList.of(new FunctionArgumentDefinition(false)),
                false,
                true,
                description,
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        int arity = functionBinding.getArity();

        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        if (arity > MAX_INPUT_VALUES) {
            throw new PrestoException(NOT_SUPPORTED, "Too many arguments for string concatenation");
        }

        MethodHandle arrayMethodHandle = methodHandle(ConcatFunction.class, "concat", Slice[].class);
        MethodHandle customMethodHandle = arrayMethodHandle.asCollector(Slice[].class, arity);

        return new ChoicesScalarFunctionImplementation(
            functionBinding,
            FAIL_ON_NULL,
            nCopies(arity, NEVER_NULL),
            customMethodHandle);
    }

    public static Slice concat(Slice[] values)
    {
        // Validate the concatenation length
        int length = 0;
        for (Slice value : values) {
            length = addExact(length, value.length());
            if (length > MAX_OUTPUT_LENGTH) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
            }
        }

        // Construct the result
        Slice result = Slices.allocate(length);
        int position = 0;
        for (Slice value : values) {
            result.setBytes(position, value);
            position += value.length();
        }

        return result;
    }
}
