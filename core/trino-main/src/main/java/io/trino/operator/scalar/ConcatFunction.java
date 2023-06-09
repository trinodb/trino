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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.addExact;
import static java.util.Collections.nCopies;

public final class ConcatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final ConcatFunction VARCHAR_CONCAT = new ConcatFunction(VARCHAR.getTypeSignature(), "Concatenates given strings");

    public static final ConcatFunction VARBINARY_CONCAT = new ConcatFunction(VARBINARY.getTypeSignature(), "concatenates given varbinary values");

    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private ConcatFunction(TypeSignature type, String description)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("concat")
                        .returnType(type)
                        .argumentType(type)
                        .variableArity()
                        .build())
                .description(description)
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        int arity = boundSignature.getArity();

        if (arity < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        MethodHandle arrayMethodHandle = methodHandle(ConcatFunction.class, "concat", Slice[].class);
        MethodHandle customMethodHandle = arrayMethodHandle.asCollector(Slice[].class, arity);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
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
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
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
