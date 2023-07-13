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
package io.trino.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.GenericHiveRecordCursorProvider;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public abstract class AbstractFileFormat
        implements FileFormat
{
    static final JobConf conf;

    static {
        conf = new JobConf(newEmptyConfiguration());
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    @Override
    public boolean supportsDate()
    {
        return true;
    }

    @Override
    public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment environment)
    {
        return Optional.empty();
    }

    @Override
    public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment environment)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        Optional<HivePageSourceFactory> pageSourceFactory = getHivePageSourceFactory(hdfsEnvironment);
        Optional<HiveRecordCursorProvider> recordCursorProvider = getHiveRecordCursorProvider(hdfsEnvironment);

        checkArgument(pageSourceFactory.isPresent() ^ recordCursorProvider.isPresent());

        if (pageSourceFactory.isPresent()) {
            return createPageSource(pageSourceFactory.get(), session, targetFile, columnNames, columnTypes, getFormat());
        }

        return createPageSource(recordCursorProvider.get(), session, targetFile, columnNames, columnTypes, getFormat());
    }

    @Override
    public ConnectorPageSource createGenericReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<ColumnHandle> readColumns,
            List<String> schemaColumnNames,
            List<Type> schemaColumnTypes)
    {
        HivePageSourceProvider factory = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                hdfsEnvironment,
                new HiveConfig(),
                getHivePageSourceFactory(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                getHiveRecordCursorProvider(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, new HiveConfig()));

        Properties schema = createSchema(getFormat(), schemaColumnNames, schemaColumnTypes);

        HiveSplit split = new HiveSplit(
                "",
                targetFile.getPath(),
                0,
                targetFile.length(),
                targetFile.length(),
                targetFile.lastModified(),
                schema,
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                false,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                SplitWeight.standard());

        return factory.createPageSource(
                TestingConnectorTransactionHandle.INSTANCE,
                session, split,
                new HiveTableHandle("schema_name", "table_name", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty()),
                readColumns,
                DynamicFilter.EMPTY);
    }

    @Override
    public boolean supports(TestData testData)
    {
        return true;
    }

    static ConnectorPageSource createPageSource(
            HiveRecordCursorProvider cursorProvider,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes should have the same size");

        List<HiveColumnHandle> readColumns = getBaseColumns(columnNames, columnTypes);

        Optional<ReaderRecordCursorWithProjections> recordCursorWithProjections = cursorProvider.createRecordCursor(
                conf,
                session,
                Location.of(targetFile.getAbsolutePath()),
                0,
                targetFile.length(),
                targetFile.length(),
                createSchema(format, columnNames, columnTypes),
                readColumns,
                TupleDomain.all(),
                TESTING_TYPE_MANAGER,
                false);

        checkState(recordCursorWithProjections.isPresent(), "readerPageSourceWithProjections is not present");
        checkState(recordCursorWithProjections.get().getProjectedReaderColumns().isEmpty(), "projection should not be required");
        return new RecordPageSource(columnTypes, recordCursorWithProjections.get().getRecordCursor());
    }

    static ConnectorPageSource createPageSource(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes should have the same size");

        List<HiveColumnHandle> readColumns = getBaseColumns(columnNames, columnTypes);

        Properties schema = createSchema(format, columnNames, columnTypes);
        Optional<ReaderPageSource> readerPageSourceWithProjections = pageSourceFactory
                .createPageSource(
                        conf,
                        session,
                        Location.of(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        schema,
                        readColumns,
                        TupleDomain.all(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        NO_ACID_TRANSACTION);

        checkState(readerPageSourceWithProjections.isPresent(), "readerPageSourceWithProjections is not present");
        checkState(readerPageSourceWithProjections.get().getReaderColumns().isEmpty(), "projection should not be required");
        return readerPageSourceWithProjections.get().get();
    }

    static List<HiveColumnHandle> getBaseColumns(List<String> columnNames, List<Type> columnTypes)
    {
        return IntStream.range(0, columnNames.size())
                .boxed()
                .map(index -> createBaseColumn(
                        columnNames.get(index),
                        index,
                        toHiveType(columnTypes.get(index)),
                        columnTypes.get(index),
                        REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());
    }

    static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, format.getSerde());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, join(",", columnNames));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(HiveType::toHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }
}
