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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.newir.Block;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.HashMap.newHashMap;
import static java.util.Objects.requireNonNull;

public record Context(Block.Builder block, Map<Symbol, RowField> symbolMapping)
{
    public Context(Block.Builder block)
    {
        this(block, Map.of());
    }

    public Context(Block.Builder block, Map<Symbol, RowField> symbolMapping)
    {
        this.block = requireNonNull(block, "block is null");
        this.symbolMapping = ImmutableMap.copyOf(requireNonNull(symbolMapping, "symbolMapping is null"));
    }

    public static Map<Symbol, RowField> argumentMapping(Block.Parameter parameter, Map<Symbol, String> symbolMapping)
    {
        return symbolMapping.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> new RowField(parameter, entry.getValue())));
    }

    public static Map<Symbol, RowField> composedMapping(Context context, Map<Symbol, RowField> newMapping)
    {
        return composedMapping(context, ImmutableList.of(newMapping));
    }

    /**
     * Compose the correlated mapping from the context with symbol mappings for the current block parameters.
     *
     * @param context rewrite context containing symbol mapping from all levels of correlation
     * @param newMappings list of symbol mappings for current block parameters
     * @return composed symbol mapping to rewrite the current block
     */
    public static Map<Symbol, RowField> composedMapping(Context context, List<Map<Symbol, RowField>> newMappings)
    {
        Map<Symbol, RowField> composed = newHashMap(context.symbolMapping().size() + newMappings.stream().mapToInt(Map::size).sum());
        composed.putAll(context.symbolMapping());
        newMappings.stream().forEach(composed::putAll);
        return composed;
    }

    public record RowField(Block.Parameter row, String field)
    {
        public RowField
        {
            requireNonNull(row, "row is null");
            requireNonNull(field, "field is null");
        }
    }
}
