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
package io.trino.plugin.paimon;

import io.trino.spi.TrinoException;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.table.PrimaryKeyTableUtils.validatePKUpsertDeletable;

public final class PaimonTableSupport
{
    private PaimonTableSupport() {}

    public static Table requireSupportedTable(Table table)
    {
        requireNonNull(table, "table is null");
        if (table instanceof VectorSearchTable) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon vector search tables are not supported by the Trino connector");
        }
        if (table instanceof FullTextSearchTable) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon full-text search tables are not supported by the Trino connector");
        }
        return table;
    }

    public static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        Table supportedTable = requireSupportedTable(table);
        if (!(supportedTable instanceof FileStoreTable fileStoreTable)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " requires FileStoreTable, but got: " + supportedTable.getClass().getName());
        }
        return fileStoreTable;
    }

    public static void validateInsertOverwrite(FileStoreTable table)
    {
        if (!table.partitionKeys().isEmpty() && !table.coreOptions().dynamicPartitionOverwrite()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
        }
    }

    public static TrinoException unsupportedBucketMode(String operation, BucketMode mode)
    {
        requireNonNull(operation, "operation is null");
        requireNonNull(mode, "mode is null");
        return switch (mode) {
            case HASH_DYNAMIC -> new TrinoException(NOT_SUPPORTED,
                    "Unsupported table bucket mode: HASH_DYNAMIC for Paimon " + operation
                            + ". HASH_DYNAMIC requires primary-key FileStoreTable writes with connector dynamic-bucket routing");
            case KEY_DYNAMIC -> new TrinoException(NOT_SUPPORTED,
                    "Unsupported table bucket mode: KEY_DYNAMIC for Paimon " + operation
                            + ". The table must be a supported Paimon FileStoreTable with a primary key");
            default -> new TrinoException(
                    NOT_SUPPORTED,
                    "Unsupported table bucket mode: " + mode + " for Paimon " + operation);
        };
    }

    public static void validateRowLevelDelete(FileStoreTable table, String operation)
    {
        requireNonNull(table, "table is null");
        requireNonNull(operation, "operation is null");
        if (table.primaryKeys().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Paimon " + operation + " requires primary keys");
        }
        try {
            validatePKUpsertDeletable(table);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageWithDetail("Paimon " + operation + " is not supported for this table", e),
                    e);
        }
    }

    private static String unsupportedOperationMessageWithDetail(String prefix, UnsupportedOperationException exception)
    {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return prefix;
        }
        return prefix + ": " + message;
    }
}
