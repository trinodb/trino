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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A relation the materialized view query depends on, as resolved by the engine when analyzing the query.
 * Views and materialized views are expanded, so each of them carries the relations its own query
 * depends on. The query of a materialized view that could not be analyzed is not expanded.
 */
public sealed interface MaterializedViewDependency
        permits MaterializedViewDependency.Foreign,
                MaterializedViewDependency.MaterializedView,
                MaterializedViewDependency.Table,
                MaterializedViewDependency.View
{
    record Table(ConnectorTableHandle handle)
            implements MaterializedViewDependency
    {
        public Table
        {
            requireNonNull(handle, "handle is null");
        }
    }

    record View(ConnectorViewHandle handle, List<MaterializedViewDependency> dependencies)
            implements MaterializedViewDependency
    {
        public View
        {
            requireNonNull(handle, "handle is null");
            dependencies = List.copyOf(requireNonNull(dependencies, "dependencies is null"));
        }
    }

    /**
     * @param storageTable the storage table of the materialized view, if it belongs to the same catalog
     * @param dependencies the relations the query of the materialized view depends on, or empty when the query could not be analyzed
     */
    record MaterializedView(ConnectorViewHandle handle, Optional<ConnectorTableHandle> storageTable, Optional<List<MaterializedViewDependency>> dependencies)
            implements MaterializedViewDependency
    {
        public MaterializedView
        {
            requireNonNull(handle, "handle is null");
            requireNonNull(storageTable, "storageTable is null");
            dependencies = requireNonNull(dependencies, "dependencies is null").map(List::copyOf);
        }
    }

    /**
     * A relation that belongs to a different catalog than the materialized view.
     */
    record Foreign(CatalogSchemaTableName name)
            implements MaterializedViewDependency
    {
        public Foreign
        {
            requireNonNull(name, "name is null");
        }
    }
}
