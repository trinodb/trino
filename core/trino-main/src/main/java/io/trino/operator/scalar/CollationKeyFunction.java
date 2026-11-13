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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.CollationComparison;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;

/// The collation key of a collated string: bytes whose unsigned order and equality are the
/// collation's. The planner groups collated strings by it, so that hashing and comparing a group
/// key compares bytes rather than applying the collation to every value.
public final class CollationKeyFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$collation_key";
    public static final CollationKeyFunction COLLATION_KEY = new CollationKeyFunction();

    private static final MethodHandle COLLATION_KEY_METHOD;

    static {
        try {
            COLLATION_KEY_METHOD = MethodHandles.lookup().findStatic(CollationKeyFunction.class, "collationKey", methodType(Slice.class, CollationComparison.class, Slice.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private CollationKeyFunction()
    {
        super(FunctionMetadata.scalarBuilder(NAME)
                .signature(Signature.builder()
                        .typeVariable("T")
                        .returnType(VARBINARY)
                        .argumentType(typeVariable("T"))
                        .build())
                .hidden()
                .neverFails()
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        // The argument is a type variable so that it binds with its collation; a varchar formal would
        // bind a collated argument as a default-collation string and compute byte keys
        if (!(boundSignature.getArgumentType(0) instanceof VarcharType varcharType)) {
            throw new TrinoException(TYPE_MISMATCH, "%s requires a varchar argument (actual: %s)".formatted(NAME, boundSignature.getArgumentType(0)));
        }
        CollationComparison comparison = collationRegistry().comparison(varcharType.getCollation());
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                List.of(NEVER_NULL),
                insertArguments(COLLATION_KEY_METHOD, 0, comparison));
    }

    private static Slice collationKey(CollationComparison comparison, Slice value)
    {
        return comparison.collationKey(value);
    }
}
