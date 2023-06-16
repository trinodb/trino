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
package io.trino.plugin.openpolicyagent.schema;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class BaseSchemaBuilder<T, B extends BaseSchemaBuilder<T, B>>
{
    public String catalogName;
    public String schemaName;
    public Map<String, Optional<Object>> properties;

    protected abstract B getInstance();

    public B catalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return getInstance();
    }

    public B schemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return getInstance();
    }

    private Optional<Object> buildOptional(Object item)
    {
        Object valueToUse = item;
        if (item instanceof Optional) {
            valueToUse = ((Optional<?>) item).orElse(null);
        }
        return Optional.ofNullable(valueToUse);
    }

    public B properties(Map<String, ?> properties)
    {
        // https://openjdk.org/jeps/269
        // ImmutableMap along with other new collections does not support null
        // cast nulls to empty optionals
        this.properties = properties
                .entrySet()
                .stream()
                .map((e) -> Map.entry(e.getKey(), buildOptional(e.getValue())))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        return getInstance();
    }

    public abstract T build();
}
