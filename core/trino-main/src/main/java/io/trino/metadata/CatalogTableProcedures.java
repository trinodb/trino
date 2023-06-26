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
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableProcedureMetadata;

import java.util.Collection;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.PROCEDURE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogTableProcedures
{
    private final Map<String, TableProcedureMetadata> procedures;

    public CatalogTableProcedures(Collection<TableProcedureMetadata> procedures)
    {
        requireNonNull(procedures, "procedures is null");
        this.procedures = Maps.uniqueIndex(procedures, TableProcedureMetadata::getName);
    }

    public Collection<TableProcedureMetadata> getTableProcedures()
    {
        return procedures.values();
    }

    public TableProcedureMetadata getTableProcedure(String name)
    {
        TableProcedureMetadata procedure = procedures.get(name);
        if (procedure == null) {
            throw new TrinoException(PROCEDURE_NOT_FOUND, "Table procedure not registered: " + name);
        }
        return procedure;
    }
}
