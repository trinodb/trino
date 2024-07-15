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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record TpchTableHandle(
        String schemaName,
        String tableName,
        double scaleFactor,
        TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle
{
    public TpchTableHandle(String schemaName, String tableName, double scaleFactor)
    {
        this(schemaName, tableName, scaleFactor, TupleDomain.all());
    }

    public TpchTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        checkArgument(scaleFactor > 0, "Scale factor must be larger than 0");
        requireNonNull(constraint, "constraint is null");
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }
}
