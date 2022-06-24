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
package io.trino.spi.connector;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ConnectorTableVersioningLayout
{
    /**
     * Columns that capture row group changes.
     */
    private final Set<ColumnHandle> versioningColumns;
    /**
     * If versioning columns are also unique row id.
     * When true, versioning in a table is on a row level (e.g. individual row updates can be captured).
     * When false, versioning in a table is on a row group (e.g. changes to entire partitions can be captured).
     */
    private final boolean unique;

    public ConnectorTableVersioningLayout(Set<ColumnHandle> versioningColumns, boolean unique)
    {
        this.versioningColumns = requireNonNull(versioningColumns, "versioningColumns is null");
        this.unique = unique;
    }

    public Set<ColumnHandle> getVersioningColumns()
    {
        return versioningColumns;
    }

    public boolean isUnique()
    {
        return unique;
    }
}
