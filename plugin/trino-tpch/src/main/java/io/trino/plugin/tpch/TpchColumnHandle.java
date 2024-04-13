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
package io.trino.plugin.tpch;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public record TpchColumnHandle(String columnName, Type type)
        implements ColumnHandle
{
    public TpchColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(type, "type is null");
    }

    @Override
    public String toString()
    {
        return "tpch:" + columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        TpchColumnHandle other = (TpchColumnHandle) o;
        return Objects.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }
}
