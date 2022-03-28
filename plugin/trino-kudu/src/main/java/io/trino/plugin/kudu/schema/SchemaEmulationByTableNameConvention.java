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
package io.trino.plugin.kudu.schema;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.kudu.KuduClientWrapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduOperationApplier;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Upsert;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.KuduClientSession.DEFAULT_SCHEMA;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class SchemaEmulationByTableNameConvention
        implements SchemaEmulation
{
    private final String commonPrefix;
    private final String rawSchemasTableName;
    private KuduTable rawSchemasTable;

    public SchemaEmulationByTableNameConvention(String commonPrefix)
    {
        this.commonPrefix = commonPrefix;
        this.rawSchemasTableName = commonPrefix + "$schemas";
    }

    @Override
    public void createSchema(KuduClientWrapper client, String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            throw new SchemaAlreadyExistsException(schemaName);
        }
        else {
            try (KuduOperationApplier operationApplier = KuduOperationApplier.fromKuduClientWrapper(client)) {
                KuduTable schemasTable = getSchemasTable(client);
                Upsert upsert = schemasTable.newUpsert();
                upsert.getRow().addString(0, schemaName);
                operationApplier.applyOperationAsync(upsert);
            }
            catch (KuduException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
    }

    @Override
    public boolean existsSchema(KuduClientWrapper client, String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            return true;
        }
        else {
            List<String> schemas = listSchemaNames(client);
            return schemas.contains(schemaName);
        }
    }

    @Override
    public void dropSchema(KuduClientWrapper client, String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Deleting default schema not allowed.");
        }
        else {
            try (KuduOperationApplier operationApplier = KuduOperationApplier.fromKuduClientWrapper(client)) {
                String prefix = getPrefixForTablesOfSchema(schemaName);
                for (String name : client.getTablesList(prefix).getTablesList()) {
                    client.deleteTable(name);
                }

                KuduTable schemasTable = getSchemasTable(client);
                Delete delete = schemasTable.newDelete();
                delete.getRow().addString(0, schemaName);
                operationApplier.applyOperationAsync(delete);
            }
            catch (KuduException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
    }

    @Override
    public List<String> listSchemaNames(KuduClientWrapper client)
    {
        try {
            if (rawSchemasTable == null) {
                createAndFillSchemasTable(client);
                rawSchemasTable = getSchemasTable(client);
            }

            KuduScanner scanner = client.newScannerBuilder(rawSchemasTable).build();
            RowResultIterator iterator = scanner.nextRows();
            ArrayList<String> result = new ArrayList<>();
            while (iterator != null) {
                for (RowResult row : iterator) {
                    result.add(row.getString(0));
                }
                iterator = scanner.nextRows();
            }
            return result;
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private KuduTable getSchemasTable(KuduClientWrapper client)
            throws KuduException
    {
        if (rawSchemasTable == null) {
            rawSchemasTable = client.openTable(rawSchemasTableName);
        }
        return rawSchemasTable;
    }

    private void createAndFillSchemasTable(KuduClientWrapper client)
            throws KuduException
    {
        List<String> existingSchemaNames = listSchemaNamesFromTablets(client);
        ColumnSchema schemaColumnSchema = new ColumnSchema.ColumnSchemaBuilder("schema", Type.STRING)
                .key(true).build();
        Schema schema = new Schema(ImmutableList.of(schemaColumnSchema));
        CreateTableOptions options = new CreateTableOptions();
        options.addHashPartitions(ImmutableList.of(schemaColumnSchema.getName()), 2);

        KuduTable schemasTable = createTableIfNotExists(client, schema, rawSchemasTableName, options);
        try (KuduOperationApplier operationApplier = KuduOperationApplier.fromKuduClientWrapper(client)) {
            for (String schemaName : existingSchemaNames) {
                Upsert upsert = schemasTable.newUpsert();
                upsert.getRow().addString(0, schemaName);
                operationApplier.applyOperationAsync(upsert);
            }
        }
    }

    private static KuduTable createTableIfNotExists(KuduClientWrapper client, Schema schema, String name, CreateTableOptions options)
            throws KuduException
    {
        try {
            return client.createTable(name, schema, options);
        }
        catch (KuduException e) {
            if (e.getStatus().isAlreadyPresent()) {
                // Table already exists
                return client.openTable(name);
            }
            throw e;
        }
    }

    private List<String> listSchemaNamesFromTablets(KuduClientWrapper client)
            throws KuduException
    {
        List<String> tables = client.getTablesList().getTablesList();
        LinkedHashSet<String> schemas = new LinkedHashSet<>();
        schemas.add(DEFAULT_SCHEMA);
        for (String table : tables) {
            SchemaTableName schemaTableName = fromRawName(table);
            if (schemaTableName != null) {
                schemas.add(schemaTableName.getSchemaName());
            }
        }
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public String toRawName(SchemaTableName schemaTableName)
    {
        if (DEFAULT_SCHEMA.equals(schemaTableName.getSchemaName())) {
            if (commonPrefix.isEmpty()) {
                if (schemaTableName.getTableName().indexOf('.') != -1) {
                    // in default schema table name must not contain dots if common prefix is empty
                    throw new TrinoException(GENERIC_USER_ERROR, "Table name conflicts with schema emulation settings. No '.' allowed for tables in schema 'default'.");
                }
            }
            else {
                if (schemaTableName.getTableName().startsWith(commonPrefix)) {
                    // in default schema table name must not start with common prefix
                    throw new TrinoException(GENERIC_USER_ERROR, "Table name conflicts with schema emulation settings. Table name must not start with '" + commonPrefix + "'.");
                }
            }
        }
        else if (schemaTableName.getSchemaName().indexOf('.') != -1) {
            // schema names with dots are not possible
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        if (DEFAULT_SCHEMA.equals(schemaTableName.getSchemaName())) {
            return schemaTableName.getTableName();
        }
        else {
            return commonPrefix + schemaTableName.getSchemaName() + "." + schemaTableName.getTableName();
        }
    }

    @Override
    public SchemaTableName fromRawName(String rawName)
    {
        if (commonPrefix.isEmpty()) {
            int dotIndex = rawName.indexOf('.');
            if (dotIndex == -1) {
                return new SchemaTableName(DEFAULT_SCHEMA, rawName);
            }
            if (dotIndex == 0 || dotIndex == rawName.length() - 1) {
                return null; // illegal rawName ignored
            }
            return new SchemaTableName(rawName.substring(0, dotIndex), rawName.substring(dotIndex + 1));
        }
        if (rawName.startsWith(commonPrefix)) {
            int start = commonPrefix.length();
            int dotIndex = rawName.indexOf('.', start);
            if (dotIndex == -1 || dotIndex == start || dotIndex == rawName.length() - 1) {
                return null; // illegal rawName ignored
            }
            String schema = rawName.substring(start, dotIndex);
            if (DEFAULT_SCHEMA.equalsIgnoreCase(schema)) {
                return null; // illegal rawName ignored
            }
            return new SchemaTableName(schema, rawName.substring(dotIndex + 1));
        }
        return new SchemaTableName(DEFAULT_SCHEMA, rawName);
    }

    @Override
    public String getPrefixForTablesOfSchema(String schemaName)
    {
        if (DEFAULT_SCHEMA.equals(schemaName)) {
            return "";
        }
        else {
            return commonPrefix + schemaName + ".";
        }
    }

    @Override
    public List<String> filterTablesForDefaultSchema(List<String> rawTables)
    {
        return rawTables.stream()
                .filter(table -> !table.contains("."))
                .collect(toImmutableList());
    }
}
