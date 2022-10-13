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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Integer.parseInt;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResolvedFunction
{
    private static final Pattern VARCHAR_MATCHER = Pattern.compile("varchar\\(([0-9]+)\\)");

    @Test
    public void test()
    {
        ResolvedFunction resolvedFunction = createResolvedFunction("top", 3);
        ResolvedFunctionDecoder decoder = new ResolvedFunctionDecoder(TestResolvedFunction::varcharTypeLoader);
        Optional<ResolvedFunction> copy = decoder.fromQualifiedName(resolvedFunction.toQualifiedName());
        assertTrue(copy.isPresent());
        assertEquals(copy.get(), resolvedFunction);
    }

    private static ResolvedFunction createResolvedFunction(String name, int depth)
    {
        return new ResolvedFunction(
                new BoundSignature(name + "_" + depth, createVarcharType(10 + depth), ImmutableList.of(createVarcharType(20 + depth), createVarcharType(30 + depth))),
                GlobalSystemConnector.CATALOG_HANDLE,
                FunctionId.toFunctionId(Signature.builder()
                        .name(name)
                        .returnType(new TypeSignature("x"))
                        .argumentType(new TypeSignature("y"))
                        .argumentType(new TypeSignature("z"))
                        .build()),
                SCALAR,
                true,
                new FunctionNullability(false, ImmutableList.of(false, false)),
                ImmutableSet.of(createVarcharType(11), createVarcharType(12), createVarcharType(13)).stream()
                        .collect(toImmutableMap(Type::getTypeSignature, Function.identity())),
                depth == 0 ? ImmutableSet.of() : ImmutableSet.of(createResolvedFunction("left", depth - 1), createResolvedFunction("right", depth - 1)));
    }

    private static Type varcharTypeLoader(TypeId typeId)
    {
        Matcher matcher = VARCHAR_MATCHER.matcher(typeId.getId());
        boolean matches = matcher.matches();
        assertTrue(matches);
        return createVarcharType(parseInt(matcher.group(1)));
    }
}
