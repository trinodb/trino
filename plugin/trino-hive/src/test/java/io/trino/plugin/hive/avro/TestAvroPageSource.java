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
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.avro.HiveAvroTypeBlockHandler;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.hive.formats.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_LITERAL;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAvroPageSource
{
    private static final Schema AVRO_SCHEMA = SchemaBuilder.record("test").fields().requiredLong("value").endRecord();
    private static final int ROW_COUNT = 1000;
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, MEGABYTE).toBytes();

    @Test
    void testReportsMemoryUsageToMemoryContext()
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test.avro");
        byte[] data = createAvroFile();
        fileSystemFactory.create(SESSION).newOutputFile(location).createOrOverwrite(data);

        AtomicLong reportedBytes = new AtomicLong(-1);
        ConnectorPageSource pageSource = new AvroPageSourceFactory(fileSystemFactory).createPageSource(
                        SESSION,
                        location,
                        0,
                        data.length,
                        data.length,
                        0,
                        new io.trino.plugin.hive.Schema(AVRO_SERDE_CLASS, false, ImmutableMap.of(SCHEMA_LITERAL, AVRO_SCHEMA.toString())),
                        ImmutableList.of(createBaseColumn("value", 0, HIVE_LONG, BIGINT, REGULAR, Optional.empty())),
                        TupleDomain.all(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        reportedBytes::set)
                .orElseThrow();
        try (pageSource) {
            assertThat(reportedBytes).hasValue(GUESSED_MEMORY_USAGE);
            // the memory must not be accounted for again when getMemoryUsage is polled
            assertThat(pageSource.getMemoryUsage()).isZero();

            int positions = 0;
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page != null) {
                    positions += page.getPositionCount();
                }
            }
            assertThat(positions).isEqualTo(ROW_COUNT);
            assertThat(reportedBytes).hasValue(GUESSED_MEMORY_USAGE);
        }
        assertThat(reportedBytes).hasValue(0);
    }

    @Test
    void testMemoryLimitExceededOnCreation()
            throws IOException
    {
        AtomicInteger openedStreams = new AtomicInteger();
        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///test.avro"), wrappedBuffer(createAvroFile()))
        {
            @Override
            public TrinoInputStream newStream()
                    throws IOException
            {
                openedStreams.incrementAndGet();
                return super.newStream();
            }
        };
        TrinoException memoryLimitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Exceeded memory limit");

        assertThatThrownBy(() -> new AvroPageSource(inputFile, AVRO_SCHEMA, new HiveAvroTypeBlockHandler(TIMESTAMP_MILLIS), 0, inputFile.length(), _ -> {
            throw memoryLimitExceeded;
        }))
                .isSameAs(memoryLimitExceeded);
        // the reservation fails before the reader opens the file, so nothing is left open
        assertThat(openedStreams).hasValue(0);
    }

    private static byte[] createAvroFile()
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(AVRO_SCHEMA))) {
            writer.create(AVRO_SCHEMA, output);
            for (long value = 0; value < ROW_COUNT; value++) {
                writer.append(new GenericRecordBuilder(AVRO_SCHEMA).set("value", value).build());
            }
        }
        return output.toByteArray();
    }
}
