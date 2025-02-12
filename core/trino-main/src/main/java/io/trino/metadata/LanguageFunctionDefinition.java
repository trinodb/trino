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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record LanguageFunctionDefinition(
        String language,
        Type returnType,
        List<Type> argumentTypes,
        String definition,
        Map<String, Object> properties)
{
    public LanguageFunctionDefinition
    {
        requireNonNull(language, "language is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        requireNonNull(definition, "definition is null");
        properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }
}
