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

import com.google.common.collect.Maps;
import io.trino.connector.CatalogName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableProcedureMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.PROCEDURE_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableProceduresRegistry
{
    private final Map<CatalogName, Map<String, TableProcedureMetadata>> tableProcedures = new ConcurrentHashMap<>();

    public void addTableProcedures(CatalogName catalogName, Collection<TableProcedureMetadata> procedures)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(procedures, "procedures is null");

        Map<String, TableProcedureMetadata> proceduresByName = Maps.uniqueIndex(procedures, TableProcedureMetadata::getName);

        checkState(tableProcedures.putIfAbsent(catalogName, proceduresByName) == null, "Table procedures already registered for connector: %s", catalogName);
    }

    public void removeProcedures(CatalogName catalogName)
    {
        tableProcedures.remove(catalogName);
    }

    public TableProcedureMetadata resolve(CatalogName catalogName, String name)
    {
        Map<String, TableProcedureMetadata> procedures = tableProcedures.get(catalogName);
        if (procedures == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Catalog %s not registered", catalogName));
        }

        TableProcedureMetadata procedure = procedures.get(name);
        if (procedure == null) {
            throw new TrinoException(PROCEDURE_NOT_FOUND, format("Procedure %s not registered for catalog %s", name, catalogName));
        }
        return procedure;
    }
}
