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
package io.trino.operator.window;

import io.trino.spi.function.Description;
import io.trino.spi.function.Signature;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSignature;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;

public final class WindowAnnotationsParser
{
    private WindowAnnotationsParser() {}

    public static List<SqlWindowFunction> parseFunctionDefinition(Class<? extends WindowFunction> clazz)
    {
        WindowFunctionSignature[] signatures = clazz.getAnnotationsByType(WindowFunctionSignature.class);
        checkArgument(signatures.length > 0, "Class is not annotated with @WindowFunctionSignature: %s", clazz.getName());
        return Stream.of(signatures)
                .map(signature -> parse(clazz, signature))
                .collect(toImmutableList());
    }

    private static SqlWindowFunction parse(Class<? extends WindowFunction> clazz, WindowFunctionSignature window)
    {
        Signature.Builder signatureBuilder = Signature.builder();
        Set<String> typeVariables = Set.of();
        if (!window.typeVariable().isEmpty()) {
            signatureBuilder.typeVariable(window.typeVariable());
            typeVariables = Set.of(window.typeVariable());
        }

        for (String type : window.argumentTypes()) {
            signatureBuilder.argumentType(parseTypeTemplate(type, typeVariables, Set.of()));
        }

        signatureBuilder.returnType(parseTypeTemplate(window.returnType(), typeVariables, Set.of()));

        Optional<String> description = Optional.ofNullable(clazz.getAnnotation(Description.class)).map(Description::value);

        boolean deprecated = clazz.getAnnotationsByType(Deprecated.class).length > 0;

        return new SqlWindowFunction(
                window.name(),
                signatureBuilder.build(),
                description,
                deprecated,
                new ReflectionWindowFunctionSupplier(window.argumentTypes().length, clazz));
    }
}
