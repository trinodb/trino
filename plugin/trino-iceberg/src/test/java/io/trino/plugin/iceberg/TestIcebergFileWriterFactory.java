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
package io.trino.plugin.iceberg;

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
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergFileWriterFactory
{
    private static final Schema SCHEMA = new Schema(Types.NestedField.optional(1, "x", Types.IntegerType.get()));
    private static final Location OUTPUT_LOCATION = Location.of("local:///data/file");

    private final IcebergFileWriterFactory fileWriterFactory = new IcebergFileWriterFactory(
            TESTING_TYPE_MANAGER,
            new NodeVersion("test"),
            new FileFormatDataSourceStats(),
            new IcebergConfig(),
            new OrcWriterConfig(),
            new ParquetWriterConfig());

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testFileDeletedWhenCreateFailsAfterCreatingFile(IcebergFileFormat fileFormat, @TempDir Path root)
            throws IOException
    {
        TrinoFileSystem fileSystem = new FailingCreateFileSystem(new LocalFileSystem(root));

        assertThatThrownBy(() -> fileWriterFactory.createDataFileWriter(fileSystem, OUTPUT_LOCATION, SCHEMA, SESSION, fileFormat, MetricsConfig.getDefault(), ImmutableMap.of()))
                .hasRootCauseMessage("create failed after creating file");

        assertThat(listFiles(fileSystem)).isEmpty();
    }

    @Test
    void testOrcFileDeletedWhenWriterConstructionFails(@TempDir Path root)
            throws IOException
    {
        TrinoFileSystem fileSystem = new LocalFileSystem(root);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new IcebergSessionProperties(
                        new IcebergConfig(),
                        new IcebergEncryptionConfig(),
                        new OrcReaderConfig(),
                        new OrcWriterConfig(),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of(
                        "orc_writer_min_stripe_size", "2MB",
                        "orc_writer_max_stripe_size", "1MB"))
                .build();

        assertThatThrownBy(() -> fileWriterFactory.createDataFileWriter(fileSystem, OUTPUT_LOCATION, SCHEMA, session, ORC, MetricsConfig.getDefault(), ImmutableMap.of()))
                .hasMessage("stripeMaxSize must be greater than or equal to stripeMinSize");

        assertThat(listFiles(fileSystem)).isEmpty();
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
