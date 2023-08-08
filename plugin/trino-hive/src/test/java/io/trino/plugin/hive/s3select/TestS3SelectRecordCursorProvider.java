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
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestS3SelectRecordCursorProvider
{
    private static final HiveColumnHandle ARTICLE_COLUMN = createBaseColumn("article", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());
    private static final HiveColumnHandle AUTHOR_COLUMN = createBaseColumn("author", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());
    private static final HiveColumnHandle DATE_ARTICLE_COLUMN = createBaseColumn("date_pub", 1, HIVE_INT, DATE, REGULAR, Optional.empty());
    private static final HiveColumnHandle QUANTITY_COLUMN = createBaseColumn("quantity", 1, HIVE_INT, INTEGER, REGULAR, Optional.empty());

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

    @Test
    public void testDisableExperimentalFeatures()
    {
        List<HiveColumnHandle> readerColumns = new ArrayList<>();
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        S3SelectRecordCursorProvider s3SelectRecordCursorProvider = new S3SelectRecordCursorProvider(
                new TestingHdfsEnvironment(new ArrayList<>()),
                new TrinoS3ClientFactory(new HiveConfig()),
                new HiveConfig().setS3SelectExperimentalPushdownEnabled(false));

        Optional<ReaderRecordCursorWithProjections> csvRecordCursor = s3SelectRecordCursorProvider.createRecordCursor(
                ConfigurationInstantiator.newEmptyConfiguration(),
                SESSION,
                Location.of("s3://fakeBucket/fakeObject.gz"),
                0,
                10,
                10,
                createTestingSchema(LazySimpleSerDe.class.getName()),
                readerColumns,
                effectivePredicate,
                TESTING_TYPE_MANAGER,
                true);
        assertTrue(csvRecordCursor.isEmpty());

        Optional<ReaderRecordCursorWithProjections> jsonRecordCursor = s3SelectRecordCursorProvider.createRecordCursor(
                ConfigurationInstantiator.newEmptyConfiguration(),
                SESSION,
                Location.of("s3://fakeBucket/fakeObject.gz"),
                0,
                10,
                10,
                createTestingSchema(JsonSerDe.class.getName()),
                readerColumns,
                effectivePredicate,
                TESTING_TYPE_MANAGER,
                true);
        assertTrue(jsonRecordCursor.isPresent());
    }

    private static Optional<ReaderRecordCursorWithProjections> getRecordCursor(TupleDomain<HiveColumnHandle> effectivePredicate,
                                                                               List<HiveColumnHandle> readerColumns,
                                                                               boolean s3SelectPushdownEnabled)
    {
        S3SelectRecordCursorProvider s3SelectRecordCursorProvider = new S3SelectRecordCursorProvider(
                new TestingHdfsEnvironment(new ArrayList<>()),
                new TrinoS3ClientFactory(new HiveConfig()),
                new HiveConfig().setS3SelectExperimentalPushdownEnabled(true));

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
        return createTestingSchema(LazySimpleSerDe.class.getName());
    }

    private static Properties createTestingSchema(String serdeClassName)
    {
        List<HiveColumnHandle> schemaColumns = getAllColumns();
        Properties schema = new Properties();
        String columnNames = buildPropertyFromColumns(schemaColumns, HiveColumnHandle::getName);
        String columnTypeNames = buildPropertyFromColumns(schemaColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        schema.setProperty(LIST_COLUMNS, columnNames);
        schema.setProperty(LIST_COLUMN_TYPES, columnTypeNames);
        schema.setProperty(SERIALIZATION_LIB, serdeClassName);
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
