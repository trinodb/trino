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
package io.prestosql.plugin.hive.s3select;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.IonSqlQueryBuilder;
import io.prestosql.plugin.hive.ReaderProjections;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.hive.ReaderProjections.projectBaseColumns;
import static io.prestosql.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;

public class S3SelectRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Set<String> CSV_SERDES = ImmutableSet.of(LazySimpleSerDe.class.getName());
    private final HdfsEnvironment hdfsEnvironment;
    private final PrestoS3ClientFactory s3ClientFactory;

    @Inject
    public S3SelectRecordCursorProvider(HdfsEnvironment hdfsEnvironment, PrestoS3ClientFactory s3ClientFactory)
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
            this.hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        Optional<ReaderProjections> projectedReaderColumns = projectBaseColumns(columns);
        // Ignore predicates on partial columns for now.
        effectivePredicate = effectivePredicate.filter((column, domain) -> column.isBaseColumn());

        String serdeName = getDeserializerClassName(schema);
        if (CSV_SERDES.contains(serdeName)) {
            List<HiveColumnHandle> readerColumns = projectedReaderColumns
                    .map(ReaderProjections::getReaderColumns)
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
