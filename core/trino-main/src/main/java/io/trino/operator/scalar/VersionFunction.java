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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;

public final class VersionFunction
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(VersionFunction.class, "getVersion", String.class);
    private final String nodeVersion;

    public VersionFunction(String nodeVersion)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("version")
                        .returnType(VARCHAR)
                        .build())
                .hidden()
                .description("Return server version")
                .build());
        this.nodeVersion = nodeVersion;
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(nodeVersion);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Slice getVersion(String version)
    {
        return utf8Slice(version);
    }
}
