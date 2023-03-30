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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ProcedureRegistry
{
    private final CatalogServiceProvider<CatalogProcedures> proceduresProvider;

    @Inject
    public ProcedureRegistry(CatalogServiceProvider<CatalogProcedures> proceduresProvider)
    {
        this.proceduresProvider = requireNonNull(proceduresProvider, "proceduresProvider is null");
    }

    public Procedure resolve(CatalogHandle catalogHandle, SchemaTableName name)
    {
        return proceduresProvider.getService(catalogHandle).getProcedure(name);
    }
}
