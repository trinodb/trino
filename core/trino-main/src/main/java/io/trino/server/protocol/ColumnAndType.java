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
package io.trino.server.protocol;

import io.trino.client.Column;
import io.trino.spi.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ColumnAndType
{
    private final int position;
    private final Column column;
    private final Type type;

    public ColumnAndType(int position, Column column, Type type)
    {
        this.position = position;
        this.column = column;
        this.type = type;
    }

    public Column getColumn()
    {
        return column;
    }

    public Type getType()
    {
        return type;
    }

    public int getPosition()
    {
        return position;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("type", type)
                .add("position", position)
                .toString();
    }
}
