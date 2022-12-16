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

import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.TableProcedureMetadata;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class TableProceduresRegistry
{
    private final CatalogServiceProvider<CatalogTableProcedures> tableProceduresProvider;

    @Inject
    public TableProceduresRegistry(CatalogServiceProvider<CatalogTableProcedures> tableProceduresProvider)
    {
        this.tableProceduresProvider = requireNonNull(tableProceduresProvider, "tableProceduresProvider is null");
    }

    public TableProcedureMetadata resolve(CatalogHandle catalogHandle, String name)
    {
        return tableProceduresProvider.getService(catalogHandle).getTableProcedure(name);
    }
}
