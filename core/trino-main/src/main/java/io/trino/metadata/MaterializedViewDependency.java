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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Engine-level counterpart of {@link io.trino.spi.connector.MaterializedViewDependency}: a relation a materialized
 * view query depends on, with views and materialized views expanded into the relations their own queries depend on.
 */
public sealed interface MaterializedViewDependency
        permits MaterializedViewDependency.MaterializedView,
                MaterializedViewDependency.Table,
                MaterializedViewDependency.View
{
    QualifiedObjectName name();

    record Table(QualifiedObjectName name, TableHandle handle)
            implements MaterializedViewDependency
    {
        public Table
        {
            requireNonNull(name, "name is null");
            requireNonNull(handle, "handle is null");
        }
    }

    record View(QualifiedObjectName name, ViewHandle handle, List<MaterializedViewDependency> dependencies)
            implements MaterializedViewDependency
    {
        public View
        {
            requireNonNull(name, "name is null");
            requireNonNull(handle, "handle is null");
            dependencies = ImmutableList.copyOf(requireNonNull(dependencies, "dependencies is null"));
        }
    }

    /**
     * @param dependencies the relations the query of the materialized view depends on, or empty when the query could not be analyzed
     */
    record MaterializedView(QualifiedObjectName name, ViewHandle handle, Optional<TableHandle> storageTable, Optional<List<MaterializedViewDependency>> dependencies)
            implements MaterializedViewDependency
    {
        public MaterializedView
        {
            requireNonNull(name, "name is null");
            requireNonNull(handle, "handle is null");
            requireNonNull(storageTable, "storageTable is null");
            dependencies = requireNonNull(dependencies, "dependencies is null").map(ImmutableList::copyOf);
        }
    }
}
