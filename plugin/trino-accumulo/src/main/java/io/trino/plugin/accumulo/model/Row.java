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
package io.trino.plugin.accumulo.model;

import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class Row
{
    private final List<Field> fields = new ArrayList<>();

    public Row() {}

    public Row addField(Field field)
    {
        requireNonNull(field, "field is null");
        fields.add(field);
        return this;
    }

    public Row addField(Object nativeValue, Type type)
    {
        requireNonNull(type, "type is null");
        fields.add(new Field(nativeValue, type));
        return this;
    }

    public Field getField(int i)
    {
        return fields.get(i);
    }

    public int length()
    {
        return fields.size();
    }

    @Override
    public String toString()
    {
        if (fields.isEmpty()) {
            return "()";
        }
        StringBuilder builder = new StringBuilder("(");
        for (Field f : fields) {
            builder.append(f).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.append(')').toString();
    }
}
