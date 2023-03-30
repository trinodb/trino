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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfosFromTypeString;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.SerdeConstants.COLUMN_NAME_DELIMITER;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static java.util.Objects.requireNonNull;

public class S3SelectRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TrinoS3ClientFactory s3ClientFactory;

    @Inject
    public S3SelectRecordCursorProvider(HdfsEnvironment hdfsEnvironment, TrinoS3ClientFactory s3ClientFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.s3ClientFactory = requireNonNull(s3ClientFactory, "s3ClientFactory is null");
    }

    @Override
    public Optional<ReaderRecordCursorWithProjections> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled)
    {
        if (!s3SelectPushdownEnabled) {
            return Optional.empty();
        }

        try {
            this.hdfsEnvironment.getFileSystem(session.getIdentity(), path, configuration);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        Optional<ReaderColumns> projectedReaderColumns = projectBaseColumns(columns);
        // Ignore predicates on partial columns for now.
        effectivePredicate = effectivePredicate.filter((column, domain) -> column.isBaseColumn());

        List<HiveColumnHandle> readerColumns = projectedReaderColumns
                .map(readColumns -> readColumns.get().stream().map(HiveColumnHandle.class::cast).collect(toImmutableList()))
                .orElseGet(() -> ImmutableList.copyOf(columns));
        // Query is not going to filter any data, no need to use S3 Select
        if (!hasFilters(schema, effectivePredicate, readerColumns)) {
            return Optional.empty();
        }

        String serdeName = getDeserializerClassName(schema);
        Optional<S3SelectDataType> s3SelectDataTypeOptional = S3SelectSerDeDataTypeMapper.getDataType(serdeName);

        if (s3SelectDataTypeOptional.isPresent()) {
            S3SelectDataType s3SelectDataType = s3SelectDataTypeOptional.get();

            IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager, s3SelectDataType);
            String ionSqlQuery = queryBuilder.buildSql(readerColumns, effectivePredicate);
            Optional<S3SelectLineRecordReader> recordReader = S3SelectLineRecordReaderProvider.get(configuration, path, start, length, schema,
                    ionSqlQuery, s3ClientFactory, s3SelectDataType);

            if (recordReader.isEmpty()) {
                // S3 Select data type is not mapped to an S3SelectLineRecordReader
                return Optional.empty();
            }

            RecordCursor cursor = new S3SelectRecordCursor<>(configuration, path, recordReader.get(), length, schema, readerColumns);
            return Optional.of(new ReaderRecordCursorWithProjections(cursor, projectedReaderColumns));
        }
        // unsupported serdes
        return Optional.empty();
    }

    private static boolean hasFilters(
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> readerColumns)
    {
        //There are no effective predicates and readercolumns and columntypes are identical to schema
        //means getting all data out of S3. We can use S3 GetObject instead of S3 SelectObjectContent in these cases.
        if (effectivePredicate.isAll()) {
            return !isEquivalentSchema(readerColumns, schema);
        }
        return true;
    }

    private static boolean isEquivalentSchema(List<HiveColumnHandle> readerColumns, Properties schema)
    {
        Set<String> projectedColumnNames = getColumnProperty(readerColumns, HiveColumnHandle::getName);
        Set<String> projectedColumnTypes = getColumnProperty(readerColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        return isEquivalentColumns(projectedColumnNames, schema) && isEquivalentColumnTypes(projectedColumnTypes, schema);
    }

    private static boolean isEquivalentColumns(Set<String> projectedColumnNames, Properties schema)
    {
        Set<String> columnNames;
        String columnNameProperty = schema.getProperty(LIST_COLUMNS);
        if (columnNameProperty.length() == 0) {
            columnNames = ImmutableSet.of();
        }
        else {
            String columnNameDelimiter = (String) schema.getOrDefault(COLUMN_NAME_DELIMITER, ",");
            columnNames = Arrays.stream(columnNameProperty.split(columnNameDelimiter))
                    .collect(toImmutableSet());
        }
        return projectedColumnNames.equals(columnNames);
    }

    private static boolean isEquivalentColumnTypes(Set<String> projectedColumnTypes, Properties schema)
    {
        String columnTypeProperty = schema.getProperty(LIST_COLUMN_TYPES);
        Set<String> columnTypes;
        if (columnTypeProperty.length() == 0) {
            columnTypes = ImmutableSet.of();
        }
        else {
            columnTypes = getTypeInfosFromTypeString(columnTypeProperty)
                    .stream()
                    .map(TypeInfo::getTypeName)
                    .collect(toImmutableSet());
        }
        return projectedColumnTypes.equals(columnTypes);
    }

    private static Set<String> getColumnProperty(List<HiveColumnHandle> readerColumns, Function<HiveColumnHandle, String> mapper)
    {
        if (readerColumns.isEmpty()) {
            return ImmutableSet.of();
        }
        return readerColumns.stream()
                .map(mapper)
                .collect(toImmutableSet());
    }
}
