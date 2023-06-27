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
package io.trino.connector;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

public class LazyCatalogSyncTask
        implements CatalogSyncTask
{
    private final AtomicReference<DefaultCatalogSyncTask> delegate = new AtomicReference<>();

    public void setCatalogSyncTask(DefaultCatalogSyncTask defaultCatalogSyncTask)
    {
        checkState(delegate.compareAndSet(null, defaultCatalogSyncTask), "catalogSyncTask already set");
    }

    @Override
    public void syncCatalogs(List<CatalogProperties> catalogsInCoordinator)
    {
        getDelegate().syncCatalogs(catalogsInCoordinator);
    }

    private DefaultCatalogSyncTask getDelegate()
    {
        DefaultCatalogSyncTask defaultCatalogSyncTask = delegate.get();
        checkState(defaultCatalogSyncTask != null, "Catalog factory is not set");
        return defaultCatalogSyncTask;
    }
}
