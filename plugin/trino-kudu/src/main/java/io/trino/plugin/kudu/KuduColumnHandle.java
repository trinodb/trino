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
package io.trino.plugin.kudu;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import static java.util.Objects.requireNonNull;

public record KuduColumnHandle(String name, int ordinalPosition, Type type)
        implements ColumnHandle
{
    public static final String ROW_ID = "row_uuid";
    public static final int ROW_ID_POSITION = -1;

    public static final KuduColumnHandle ROW_ID_HANDLE = new KuduColumnHandle(ROW_ID, ROW_ID_POSITION, VarbinaryType.VARBINARY);

    public KuduColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, type);
    }

    public boolean isVirtualRowId()
    {
        return name.equals(ROW_ID);
    }
}
