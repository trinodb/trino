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
package io.trino.plugin.paimon.testing;

import io.trino.plugin.paimon.PaimonTableOptionUtils;
import io.trino.plugin.paimon.PaimonTableOptions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.paimon.PaimonTypeUtils.toPaimonType;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static java.lang.String.format;

/**
 * Utility class for Paimon connector testing.
 * This class provides helper methods for creating tables in tests.
 */
public final class PaimonTestingUtils
{
    private PaimonTestingUtils() {}

    /**
     * Create a Paimon table for testing purposes.
     * This method is intended for use in tests only.
     */
    public static void createPaimonTable(
            Catalog catalog,
            ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());

        try {
            catalog.createTable(identifier, prepareSchema(tableMetadata), true);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema %s not found", table.getSchemaName()), e);
        }
        catch (Catalog.TableAlreadyExistException _) {
        }
    }

    private static Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder = Schema.newBuilder()
                .primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties))
                .comment(tableMetadata.getComment().orElse(null));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(column.getName(), toPaimonType(column.getType()), column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }
}
