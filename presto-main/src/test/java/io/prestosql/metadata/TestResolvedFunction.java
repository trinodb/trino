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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Integer.parseInt;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResolvedFunction
{
    private static final Pattern VARCHAR_MATCHER = Pattern.compile("varchar\\(([0-9]+)\\)");

    @Test
    public void test()
    {
        ResolvedFunction resolvedFunction = createResolvedFunction("top");
        ResolvedFunctionDecoder decoder = new ResolvedFunctionDecoder(TestResolvedFunction::varcharTypeLoader);
        Optional<ResolvedFunction> copy = decoder.fromQualifiedName(resolvedFunction.toQualifiedName());
        assertTrue(copy.isPresent());
        assertEquals(copy.get(), resolvedFunction);
    }

    private static ResolvedFunction createResolvedFunction(String name)
    {
        return new ResolvedFunction(
                new BoundSignature(name, createVarcharType(10), ImmutableList.of(createVarcharType(20), createVarcharType(30))),
                FunctionId.toFunctionId(Signature.builder()
                        .name(name)
                        .returnType(new TypeSignature("x"))
                        .argumentTypes(new TypeSignature("y"), new TypeSignature("z"))
                        .build()));
    }

    private static Type varcharTypeLoader(TypeId typeId)
    {
        Matcher matcher = VARCHAR_MATCHER.matcher(typeId.getId());
        boolean matches = matcher.matches();
        assertTrue(matches);
        return createVarcharType(parseInt(matcher.group(1)));
    }
}
