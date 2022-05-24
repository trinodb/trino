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

package io.trino.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.sql.planner.Symbol;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TypedSymbol
{
    private final Symbol symbol;
    private final String type;

    @JsonCreator
    public TypedSymbol(@JsonProperty("symbol") Symbol symbol, @JsonProperty("type") String type)
    {
        this.symbol = requireNonNull(symbol, "symbol is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public Symbol getSymbol()
    {
        return symbol;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(symbol, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TypedSymbol o = (TypedSymbol) obj;
        return symbol.equals(o.symbol)
                && type.equals(o.type);
    }
}
