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
import io.trino.filesystem.Location;
import io.trino.hadoop.ConfigurationInstantiator;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.trino.plugin.hive.TestBackgroundHiveSplitLoader.TestingHdfsEnvironment;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
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
    @Test
    public void shouldReturnSelectRecordCursor()
    {
        List<HiveColumnHandle> readerColumns = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, readerColumns, true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenEffectivePredicateExists()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = withColumnDomains(ImmutableMap.of(QUANTITY_COLUMN,
                Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, getAllColumns(), true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnSelectRecordCursorWhenProjectionExists()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        List<HiveColumnHandle> readerColumns = ImmutableList.of(QUANTITY_COLUMN, AUTHOR_COLUMN, ARTICLE_COLUMN);
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, readerColumns, true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenPushdownIsDisabled()
    {
        List<HiveColumnHandle> readerColumns = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, readerColumns, false);
        assertTrue(recordCursor.isEmpty());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenQueryIsNotFiltering()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, getAllColumns(), true);
        assertTrue(recordCursor.isEmpty());
    }

    @Test
    public void shouldNotReturnSelectRecordCursorWhenProjectionOrderIsDifferent()
    {
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        List<HiveColumnHandle> readerColumns = ImmutableList.of(DATE_ARTICLE_COLUMN, QUANTITY_COLUMN, ARTICLE_COLUMN, AUTHOR_COLUMN);
        Optional<ReaderRecordCursorWithProjections> recordCursor =
                getRecordCursor(effectivePredicate, readerColumns, true);
        assertTrue(recordCursor.isEmpty());
    }

    private static Optional<ReaderRecordCursorWithProjections> getRecordCursor(TupleDomain<HiveColumnHandle> effectivePredicate,
                                                                               List<HiveColumnHandle> readerColumns,
                                                                               boolean s3SelectPushdownEnabled)
    {
        S3SelectRecordCursorProvider s3SelectRecordCursorProvider = new S3SelectRecordCursorProvider(
                new TestingHdfsEnvironment(new ArrayList<>()),
                new TrinoS3ClientFactory(new HiveConfig()));

        return s3SelectRecordCursorProvider.createRecordCursor(
                ConfigurationInstantiator.newEmptyConfiguration(),
                SESSION,
                Location.of("s3://fakeBucket/fakeObject.gz"),
                0,
                10,
                10,
                createTestingSchema(),
                readerColumns,
                effectivePredicate,
                TESTING_TYPE_MANAGER,
                s3SelectPushdownEnabled);
    }

    private static Properties createTestingSchema()
    {
        List<HiveColumnHandle> schemaColumns = getAllColumns();
        Properties schema = new Properties();
        String columnNames = buildPropertyFromColumns(schemaColumns, HiveColumnHandle::getName);
        String columnTypeNames = buildPropertyFromColumns(schemaColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        schema.setProperty(LIST_COLUMNS, columnNames);
        schema.setProperty(LIST_COLUMN_TYPES, columnTypeNames);
        String deserializerClassName = LazySimpleSerDe.class.getName();
        schema.setProperty(SERIALIZATION_LIB, deserializerClassName);
        return schema;
    }

    private static String buildPropertyFromColumns(List<HiveColumnHandle> columns, Function<HiveColumnHandle, String> mapper)
    {
        if (columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(mapper)
                .collect(Collectors.joining(","));
    }

    private static List<HiveColumnHandle> getAllColumns()
    {
        return ImmutableList.of(ARTICLE_COLUMN, AUTHOR_COLUMN, DATE_ARTICLE_COLUMN, QUANTITY_COLUMN);
    }
}
