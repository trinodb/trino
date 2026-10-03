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
package io.trino.plugin.iceberg.catalog.rest;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.TableScan;

/**
 * A table that keeps Trino's quoted table name convention while delegating scan creation to a
 * table loaded from the REST catalog. Used when the catalog requested server-side scan planning
 * for the table: recreating a plain {@link BaseTable} would discard the scan implementation and
 * silently fall back to local planning.
 */
final class ServerPlannedTable
        extends BaseTable
{
    private final BaseTable delegate;

    ServerPlannedTable(BaseTable delegate, String name)
    {
        super(delegate.operations(), name, delegate.reporter());
        this.delegate = delegate;
    }

    @Override
    public TableScan newScan()
    {
        return delegate.newScan();
    }

    @Override
    public BatchScan newBatchScan()
    {
        return delegate.newBatchScan();
    }

    @Override
    public boolean allowDistributedPlanning()
    {
        return delegate.allowDistributedPlanning();
    }
}
