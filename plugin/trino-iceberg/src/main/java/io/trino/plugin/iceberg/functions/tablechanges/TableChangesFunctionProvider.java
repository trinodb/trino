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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProvider
        implements Provider<ConnectorTableFunction>
{
    private final TrinoCatalogFactory trinoCatalogFactory;
    private final TypeManager typeManager;

    @Inject
    public TableChangesFunctionProvider(TrinoCatalogFactory trinoCatalogFactory, TypeManager typeManager)
    {
        this.trinoCatalogFactory = requireNonNull(trinoCatalogFactory, "trinoCatalogFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(
                new TableChangesFunction(trinoCatalogFactory, typeManager),
                getClass().getClassLoader());
    }
}
