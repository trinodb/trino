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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.WriterKind;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalInt;

import static io.trino.plugin.hive.HiveCompressionCodec.NONE;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestOrcFileWriterFactory
{
    private static final Location OUTPUT_LOCATION = Location.of("local:///data/file");

    @Test
    void testFileDeletedWhenCreateFailsAfterCreatingFile(@TempDir Path root)
            throws IOException
    {
        TrinoFileSystem fileSystem = new FailingCreateFileSystem(new LocalFileSystem(root));

        assertThatThrownBy(() -> createFileWriter(fileSystem, SESSION))
                .hasRootCauseMessage("create failed after creating file");

        assertThat(listFiles(fileSystem)).isEmpty();
    }

    @Test
    void testFileDeletedWhenWriterConstructionFails(@TempDir Path root)
            throws IOException
    {
        TrinoFileSystem fileSystem = new LocalFileSystem(root);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(new HiveConfig(), new OrcReaderConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        "orc_optimized_writer_min_stripe_size", "2MB",
                        "orc_optimized_writer_max_stripe_size", "1MB"))
                .build();

        assertThatThrownBy(() -> createFileWriter(fileSystem, session))
                .hasMessage("stripeMaxSize must be greater than or equal to stripeMinSize");

        assertThat(listFiles(fileSystem)).isEmpty();
    }

    private static void createFileWriter(TrinoFileSystem fileSystem, ConnectorSession session)
    {
        new OrcFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), new FileFormatDataSourceStats(), new OrcWriterOptions(), _ -> fileSystem)
                .createFileWriter(
                        OUTPUT_LOCATION,
                        ImmutableList.of("value"),
                        ORC.toStorageFormat(),
                        NONE,
                        ImmutableMap.of(LIST_COLUMNS, "value", LIST_COLUMN_TYPES, "string"),
                        session,
                        OptionalInt.empty(),
                        NO_ACID_TRANSACTION,
                        false,
                        WriterKind.INSERT);
    }

    private static List<Location> listFiles(TrinoFileSystem fileSystem)
            throws IOException
    {
        ImmutableList.Builder<Location> files = ImmutableList.builder();
        FileIterator iterator = fileSystem.listFiles(Location.of("local:///"));
        while (iterator.hasNext()) {
            files.add(iterator.next().location());
        }
        return files.build();
    }

    private static final class FailingCreateFileSystem
            extends ForwardingTrinoFileSystem
    {
        public FailingCreateFileSystem(TrinoFileSystem delegate)
        {
            super(delegate);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            TrinoOutputFile delegate = super.newOutputFile(location);
            return new TrinoOutputFile()
            {
                @Override
                public TrinoOutputStream create(AggregatedMemoryContext memoryContext)
                        throws IOException
                {
                    delegate.create(memoryContext).close();
                    throw new IOException("create failed after creating file");
                }

                @Override
                public void createOrOverwrite(byte[] data)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Location location()
                {
                    return delegate.location();
                }
            };
        }
    }
}
