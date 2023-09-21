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
package io.trino.plugin.deltalake;

import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class TestingDeltaLakeUtils
{
    private TestingDeltaLakeUtils() {}

    public static <T> T getConnectorService(DistributedQueryRunner queryRunner, Class<T> clazz)
    {
        return ((DeltaLakeConnector) queryRunner.getCoordinator().getConnector(DELTA_CATALOG)).getInjector().getInstance(clazz);
    }

    public static List<AddFileEntry> getTableActiveFiles(TransactionLogAccess transactionLogAccess, String tableLocation)
            throws IOException
    {
        SchemaTableName dummyTable = new SchemaTableName("dummy_schema_placeholder", "dummy_table_placeholder");

        // force entries to have JSON serializable statistics
        transactionLogAccess.flushCache();

        TableSnapshot snapshot = transactionLogAccess.loadSnapshot(dummyTable, tableLocation, SESSION);
        return transactionLogAccess.getActiveFiles(snapshot, SESSION);
    }

    public static void copyDirectoryContents(Path source, Path destination)
            throws IOException
    {
        try (Stream<Path> stream = Files.walk(source)) {
            stream.forEach(file -> {
                try {
                    Files.copy(file, destination.resolve(source.relativize(file)), REPLACE_EXISTING);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
