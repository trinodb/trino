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
package io.trino.sql.planner;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;

import static java.util.Objects.requireNonNull;

@JsonSerialize(keyUsing = SymbolKeySerializer.class)
public record Symbol(Type type, String name)
{
    public static Symbol from(Expression expression)
    {
        if (!(expression instanceof Reference symbol)) {
            throw new IllegalArgumentException("Unexpected expression: " + expression);
        }
        return new Symbol(symbol.type(), symbol.name());
    }

    public Symbol
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public Reference toSymbolReference()
    {
        return new Reference(type, name);
    }

    @Override
    public String toString()
    {
        return name + "::[" + type + "]";
    }
}
