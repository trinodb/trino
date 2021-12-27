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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class S3SelectRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Set<String> CSV_SERDES = ImmutableSet.of(LazySimpleSerDe.class.getName());
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

        String serdeName = getDeserializerClassName(schema);
        if (CSV_SERDES.contains(serdeName)) {
            List<HiveColumnHandle> readerColumns = projectedReaderColumns
                    .map(ReaderColumns::get)
                    .map(readColumns -> readColumns.stream().map(HiveColumnHandle.class::cast).collect(toUnmodifiableList()))
                    .orElse(columns);

            IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager);
            String ionSqlQuery = queryBuilder.buildSql(readerColumns, effectivePredicate);
            S3SelectLineRecordReader recordReader = new S3SelectCsvRecordReader(configuration, path, start, length, schema, ionSqlQuery, s3ClientFactory);

            RecordCursor cursor = new S3SelectRecordCursor<>(configuration, path, recordReader, length, schema, readerColumns);
            return Optional.of(new ReaderRecordCursorWithProjections(cursor, projectedReaderColumns));
        }

        // unsupported serdes
        return Optional.empty();
    }
}
