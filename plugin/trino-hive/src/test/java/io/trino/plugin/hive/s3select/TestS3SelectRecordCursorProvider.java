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
import com.google.common.collect.ImmutableMap;
import io.trino.hadoop.ConfigurationInstantiator;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.TestBackgroundHiveSplitLoader.TestingHdfsEnvironment;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.s3select.TestS3SelectRecordCursor.ARTICLE_COLUMN;
import static io.trino.plugin.hive.s3select.TestS3SelectRecordCursor.AUTHOR_COLUMN;
import static io.trino.plugin.hive.s3select.TestS3SelectRecordCursor.DATE_ARTICLE_COLUMN;
import static io.trino.plugin.hive.s3select.TestS3SelectRecordCursor.QUANTITY_COLUMN;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestS3SelectRecordCursorProvider
{
    private static final Configuration CONFIGURATION = ConfigurationInstantiator.newEmptyConfiguration();
    private static final Path PATH = new Path("s3://fakeBucket/fakeObject.gz");
    private static final long START = 0;
    private static final long LENGTH = 10;
    private static final long FILESIZE = 10;
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new TestingHdfsEnvironment(new ArrayList<>());
    private static final S3SelectRecordCursorProvider S3_SELECT_RECORD_CURSOR_PROVIDER = new S3SelectRecordCursorProvider(HDFS_ENVIRONMENT, new TrinoS3ClientFactory(new HiveConfig()));
    private static boolean s3SelectPushdownEnabled = true;
    private static final List<HiveColumnHandle> SCHEMA_COLUMNS = ImmutableList.of(ARTICLE_COLUMN, AUTHOR_COLUMN, DATE_ARTICLE_COLUMN, QUANTITY_COLUMN);
    private static final Properties SCHEMA = createTestingSchema();

    @Test
    public void shouldReturnSelectRecordCursor()
    {
        List<HiveColumnHandle> columnHandleList = new ArrayList<>();
        s3SelectPushdownEnabled = true;
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, columnHandleList, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenEffectivePredicateExists()
    {
        s3SelectPushdownEnabled = true;
        TupleDomain<HiveColumnHandle> effectivePredicate = withColumnDomains(ImmutableMap.of(QUANTITY_COLUMN,
                Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, SCHEMA_COLUMNS, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenProjectionExists()
    {
        s3SelectPushdownEnabled = true;
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        final List<HiveColumnHandle> readerColumns = ImmutableList.of(QUANTITY_COLUMN, AUTHOR_COLUMN, ARTICLE_COLUMN);
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, readerColumns, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenPushdownIsDisabled()
    {
        s3SelectPushdownEnabled = false;
        List<HiveColumnHandle> columnHandleList = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, columnHandleList, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isEmpty());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenQueryIsNotFiltering()
    {
        s3SelectPushdownEnabled = true;
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, SCHEMA_COLUMNS, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isEmpty());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenProjectionOrderIsDifferent()
    {
        s3SelectPushdownEnabled = true;
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        final List<HiveColumnHandle> readerColumns = ImmutableList.of(DATE_ARTICLE_COLUMN, QUANTITY_COLUMN, ARTICLE_COLUMN, AUTHOR_COLUMN);
        Optional<HiveRecordCursorProvider.ReaderRecordCursorWithProjections> recordCursor =
                S3_SELECT_RECORD_CURSOR_PROVIDER.createRecordCursor(
                        CONFIGURATION, SESSION, PATH, START, LENGTH, FILESIZE, SCHEMA, readerColumns, effectivePredicate, TESTING_TYPE_MANAGER, s3SelectPushdownEnabled);
        assertTrue(recordCursor.isEmpty());
    }

    private static Properties createTestingSchema()
    {
        Properties schema = new Properties();
        String columnNames = buildPropertyFromColumns(SCHEMA_COLUMNS, HiveColumnHandle::getName);
        String columnTypeNames = buildPropertyFromColumns(SCHEMA_COLUMNS, column -> column.getHiveType().getTypeInfo().getTypeName());
        schema.setProperty(LIST_COLUMNS, columnNames);
        schema.setProperty(LIST_COLUMN_TYPES, columnTypeNames);
        String deserializerClassName = LazySimpleSerDe.class.getName();
        schema.setProperty(SERIALIZATION_LIB, deserializerClassName);
        return schema;
    }

    private static String buildPropertyFromColumns(List<HiveColumnHandle> columns, Function<HiveColumnHandle, String> mapper)
    {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(mapper)
                .collect(Collectors.joining(","));
    }
}
