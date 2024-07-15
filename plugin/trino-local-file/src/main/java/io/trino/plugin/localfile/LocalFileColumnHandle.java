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
package io.trino.plugin.localfile;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record LocalFileColumnHandle(String columnName, Type columnType, int ordinalPosition)
        implements ColumnHandle
{
    public static final int SERVER_ADDRESS_ORDINAL_POSITION = -1;
    public static final String SERVER_ADDRESS_COLUMN_NAME = "server_address";

    public LocalFileColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }
}
