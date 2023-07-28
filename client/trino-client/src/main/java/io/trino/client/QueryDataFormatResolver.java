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
package io.trino.client;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class QueryDataFormatResolver
{
    private final Map<String, Class<? extends QueryData>> formats;

    public QueryDataFormatResolver(List<Class<? extends QueryData>> dataFormats)
    {
        this.formats = dataFormats.stream()
                // This will throw an exception if there are two queryFormats with the same name
                .collect(toImmutableMap(QueryDataFormatResolver::formatNameForClass, identity()));
    }

    public static QueryDataFormatResolver jsonInlineOnlyResolver()
    {
        // Can be empty, JsonInlineQueryData has a special handling in the deserializer/serializer
        return new QueryDataFormatResolver(ImmutableList.of());
    }

    public Class<?> getClassByFormatName(String formatName)
    {
        return formats.get(formatName);
    }

    public Set<String> supportedFormats()
    {
        return formats.keySet();
    }

    public static String formatNameForClass(Class<?> clazz)
    {
        String className = clazz.getSimpleName();
        checkArgument(className.matches("^[a-zA-Z\\-]+QueryData$"), "Name of %s should end with 'QueryData'", clazz);
        return requireNonNull(clazz.getAnnotation(QueryDataFormat.class), format("@QueryDataFormat is missing on %s", clazz)).formatName();
    }

    public String formatNameFromObject(Object object)
    {
        return formatNameForClass(object.getClass());
    }
}
