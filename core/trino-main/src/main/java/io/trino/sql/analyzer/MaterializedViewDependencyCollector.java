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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.MaterializedViewDependency;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.ViewHandle;
import io.trino.spi.TrinoException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Builds the tree of relations a materialized view query depends on while the query is analyzed.
 * Views and materialized views are opened before their query is analyzed and closed afterward, so
 * every relation is attached to the innermost view being expanded.
 */
final class MaterializedViewDependencyCollector
{
    private final Set<MaterializedViewDependency> rootDependencies = new LinkedHashSet<>();
    private final Deque<OpenView> openViews = new ArrayDeque<>();

    public void addTable(QualifiedObjectName name, TableHandle handle)
    {
        currentDependencies().add(new MaterializedViewDependency.Table(name, handle));
    }

    public void beginView(QualifiedObjectName name, ViewHandle handle)
    {
        openViews.push(new OpenView(name, handle, Optional.empty(), false));
    }

    /**
     * Records a materialized view with the relations its query depends on, collected while running {@code expansion}.
     * If the query can no longer be analyzed, e.g. because a table it reads was dropped, and the materialized view has a storage table,
     * the materialized view is recorded without dependencies and an empty result is returned.
     */
    public <T> Optional<T> expandMaterializedView(QualifiedObjectName name, ViewHandle handle, Optional<TableHandle> storageTable, Supplier<T> expansion)
    {
        int depth = openViews.size();
        openViews.push(new OpenView(name, handle, storageTable, true));
        try {
            T result = expansion.get();
            endView();
            return Optional.of(result);
        }
        catch (TrinoException e) {
            if (storageTable.isEmpty()) {
                throw e;
            }
            // Discard the relations collected so far, including the views left open by the failed analysis
            while (openViews.size() > depth + 1) {
                openViews.pop();
            }
            OpenView view = openViews.pop();
            currentDependencies().add(new MaterializedViewDependency.MaterializedView(view.name(), view.handle(), view.storageTable(), Optional.empty()));
            return Optional.empty();
        }
    }

    public void endView()
    {
        checkState(!openViews.isEmpty(), "No view is open");
        OpenView view = openViews.pop();
        List<MaterializedViewDependency> dependencies = ImmutableList.copyOf(view.dependencies());
        MaterializedViewDependency dependency;
        if (view.materialized()) {
            dependency = new MaterializedViewDependency.MaterializedView(view.name(), view.handle(), view.storageTable(), Optional.of(dependencies));
        }
        else {
            dependency = new MaterializedViewDependency.View(view.name(), view.handle(), dependencies);
        }
        currentDependencies().add(dependency);
    }

    public List<MaterializedViewDependency> build()
    {
        checkState(openViews.isEmpty(), "Views are still open: %s", openViews);
        return ImmutableList.copyOf(rootDependencies);
    }

    private Set<MaterializedViewDependency> currentDependencies()
    {
        if (openViews.isEmpty()) {
            return rootDependencies;
        }
        return openViews.peek().dependencies();
    }

    private record OpenView(QualifiedObjectName name, ViewHandle handle, Optional<TableHandle> storageTable, boolean materialized, Set<MaterializedViewDependency> dependencies)
    {
        private OpenView(QualifiedObjectName name, ViewHandle handle, Optional<TableHandle> storageTable, boolean materialized)
        {
            this(name, handle, storageTable, materialized, new LinkedHashSet<>());
        }

        private OpenView
        {
            requireNonNull(name, "name is null");
            requireNonNull(handle, "handle is null");
            requireNonNull(storageTable, "storageTable is null");
            requireNonNull(dependencies, "dependencies is null");
        }
    }
}
