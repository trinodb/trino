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
package io.trino.testing.statistics;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class StatsContext
{
    private final Map<String, Symbol> columnSymbols;

    public StatsContext(Map<String, Symbol> columnSymbols)
    {
        this.columnSymbols = ImmutableMap.copyOf(columnSymbols);
    }

    public Symbol getSymbolForColumn(String columnName)
    {
        checkArgument(columnSymbols.containsKey(columnName), "no symbol found for column '%s'", columnName);
        return columnSymbols.get(columnName);
    }

    public Type getTypeForSymbol(Symbol symbol)
    {
        return symbol.type();
    }
}
