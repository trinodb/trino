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
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.longVariableExpression;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public final class ConcatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final ConcatFunction[] VARCHAR_CONCAT_FUNCTIONS;

    public static final ConcatFunction VARBINARY_CONCAT = new ConcatFunction();

    private static final int MIN_INPUT_VALUES = 1;
    private static final int MAX_INPUT_VALUES = 254;
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    static {
        VARCHAR_CONCAT_FUNCTIONS = new ConcatFunction[MAX_INPUT_VALUES - MIN_INPUT_VALUES + 1];

        for (int arity = MIN_INPUT_VALUES; arity <= MAX_INPUT_VALUES; arity++) {
            VARCHAR_CONCAT_FUNCTIONS[arity - MIN_INPUT_VALUES] = new ConcatFunction(arity);
        }
    }

    // Concat function for VARCHAR type
    private ConcatFunction(int arity)
    {
        super(new FunctionMetadata(
                new Signature(
                        "concat",
                        ImmutableList.of(),
                        ImmutableList.of(longVariableExpression("L", toExpression(arity))),
                        new TypeSignature(VARCHAR.getTypeSignature().getBase(), typeVariable("L")),
                        toArgumentTypes(VARCHAR.getTypeSignature().getBase(), arity),
                        false),
                false,
                nCopies(arity, new FunctionArgumentDefinition(false)),
                false,
                true,
                "Concatenates given strings",
                SCALAR));
    }

    // Concat function for VARBINARY type
    private ConcatFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "concat",
                        ImmutableList.of(),
                        ImmutableList.of(),
                        VARBINARY.getTypeSignature(),
                        ImmutableList.of(VARBINARY.getTypeSignature()),
                        true),
                false,
                ImmutableList.of(new FunctionArgumentDefinition(false)),
                false,
                true,
                "concatenates given varbinary values",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        int arity = functionBinding.getArity();

        if (arity < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        if (arity > MAX_INPUT_VALUES) {
            throw new TrinoException(NOT_SUPPORTED, "Too many arguments for string concatenation");
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

    private static String toExpression(int arity)
    {
        return format("min(2147483647, %s)", IntStream.rangeClosed(1, arity).mapToObj(number -> "S" + number).collect(Collectors.joining("+")));
    }

    private static List<TypeSignature> toArgumentTypes(String base, int arity)
    {
        return IntStream.rangeClosed(1, arity)
                .mapToObj(number -> new TypeSignature(base, typeVariable("S" + number)))
                .collect(toImmutableList());
    }
}
