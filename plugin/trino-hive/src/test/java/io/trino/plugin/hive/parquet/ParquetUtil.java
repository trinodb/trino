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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.Schema;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;

final class ParquetUtil
{
    private ParquetUtil() {}

    public static ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, List<String> columnNames, List<Type> columnTypes)
            throws IOException
    {
        return createPageSource(session, parquetFile, getBaseColumns(columnNames, columnTypes), TupleDomain.all());
    }

    public static ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, List<String> columnNames, List<Type> columnTypes, DateTimeZone timeZone)
            throws IOException
    {
        return createPageSource(session, parquetFile, getBaseColumns(columnNames, columnTypes), TupleDomain.all(), new HiveConfig().setParquetTimeZone(timeZone.toString()));
    }

    public static ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> domain, DateTimeZone timeZone)
            throws IOException
    {
        return createPageSource(session, parquetFile, columns, domain, new HiveConfig().setParquetTimeZone(timeZone.toString()));
    }

    public static ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> domain)
            throws IOException
    {
        return createPageSource(session, parquetFile, columns, domain, new HiveConfig());
    }

    private static ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> domain, HiveConfig hiveConfig)
            throws IOException
    {
        // copy the test file into the memory filesystem
        MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test.file");
        try (OutputStream out = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newOutputFile(location).create()) {
            out.write(Files.readAllBytes(parquetFile.toPath()));
        }

        HivePageSourceFactory hivePageSourceFactory = new ParquetPageSourceFactory(
                fileSystemFactory,
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig(),
                hiveConfig);

        return hivePageSourceFactory.createPageSource(
                        session,
                        location,
                        0,
                        parquetFile.length(),
                        parquetFile.length(),
                        parquetFile.lastModified(),
                        new Schema(HiveStorageFormat.PARQUET.getSerde(), false, ImmutableMap.of()),
                        columns,
                        domain,
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        NO_ACID_TRANSACTION)
                .orElseThrow()
                .get();
    }

    private static List<HiveColumnHandle> getBaseColumns(List<String> columnNames, List<Type> columnTypes)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes should have the same size");

        return IntStream.range(0, columnNames.size())
                .mapToObj(index -> createBaseColumn(
                        columnNames.get(index),
                        index,
                        toHiveType(columnTypes.get(index)),
                        columnTypes.get(index),
                        REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());
    }
}
