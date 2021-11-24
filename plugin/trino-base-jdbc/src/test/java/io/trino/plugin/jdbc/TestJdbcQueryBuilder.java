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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.SqlTime;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertContains;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BOOLEAN;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_CHAR;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DATE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_SMALLINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TIME;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TIMESTAMP;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TINYINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilder
{
    private static final JdbcNamedRelationHandle TEST_TABLE = new JdbcNamedRelationHandle(new SchemaTableName(
            "some_test_schema", "test_table"),
            new RemoteTableName(Optional.empty(), Optional.empty(), "test_table"));
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
            .build();

    private TestingDatabase database;
    private JdbcClient jdbcClient;

    private List<JdbcColumnHandle> columns;

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabase();

        jdbcClient = database.getJdbcClient();

        CharType charType = CharType.createCharType(0);

        columns = ImmutableList.of(
                new JdbcColumnHandle("col_0", JDBC_BIGINT, BIGINT),
                new JdbcColumnHandle("col_1", JDBC_DOUBLE, DOUBLE),
                new JdbcColumnHandle("col_2", JDBC_BOOLEAN, BOOLEAN),
                new JdbcColumnHandle("col_3", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("col_4", JDBC_DATE, DATE),
                new JdbcColumnHandle("col_5", JDBC_TIME, TIME),
                new JdbcColumnHandle("col_6", JDBC_TIMESTAMP, TIMESTAMP_MILLIS),
                new JdbcColumnHandle("col_7", JDBC_TINYINT, TINYINT),
                new JdbcColumnHandle("col_8", JDBC_SMALLINT, SMALLINT),
                new JdbcColumnHandle("col_9", JDBC_INTEGER, INTEGER),
                new JdbcColumnHandle("col_10", JDBC_REAL, REAL),
                new JdbcColumnHandle("col_11", JDBC_CHAR, charType));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("create table \"test_table\" (" +
                "\"col_0\" BIGINT, " +
                "\"col_1\" DOUBLE, " +
                "\"col_2\" BOOLEAN, " +
                "\"col_3\" VARCHAR(128), " +
                "\"col_4\" DATE, " +
                "\"col_5\" TIME, " +
                "\"col_6\" TIMESTAMP, " +
                "\"col_7\" TINYINT, " +
                "\"col_8\" SMALLINT, " +
                "\"col_9\" INTEGER, " +
                "\"col_10\" REAL, " +
                "\"col_11\" CHAR(128) " +
                ")")) {
            preparedStatement.execute();
            StringBuilder stringBuilder = new StringBuilder("insert into \"test_table\" values ");
            int len = 1000;
            LocalDateTime dateTime = LocalDateTime.of(2016, 3, 23, 12, 23, 37);
            for (int i = 0; i < len; i++) {
                stringBuilder.append(format(
                        Locale.ENGLISH,
                        "(%d, %f, %b, 'test_str_%d', '%s', '%s', '%s', %d, %d, %d, %f, 'test_str_%d')",
                        i,
                        200000.0 + i / 2.0,
                        i % 2 == 0,
                        i,
                        Date.valueOf(dateTime.toLocalDate()),
                        Time.valueOf(dateTime.toLocalTime()),
                        Timestamp.valueOf(dateTime),
                        i % 128,
                        -i,
                        i - 100,
                        100.0f + i,
                        i));
                dateTime = dateTime.plusHours(26);
                if (i != len - 1) {
                    stringBuilder.append(",");
                }
            }
            try (PreparedStatement preparedStatement2 = connection.prepareStatement(stringBuilder.toString())) {
                preparedStatement2.execute();
            }
        }
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testNormalBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(columns.get(0), Domain.create(SortedRangeSet.copyOf(BIGINT,
                        ImmutableList.of(
                                Range.equal(BIGINT, 128L),
                                Range.equal(BIGINT, 180L),
                                Range.equal(BIGINT, 233L),
                                Range.lessThan(BIGINT, 25L),
                                Range.range(BIGINT, 66L, true, 96L, true),
                                Range.greaterThan(BIGINT, 192L))),
                        false))
                .put(columns.get(1), Domain.create(SortedRangeSet.copyOf(DOUBLE,
                        ImmutableList.of(
                                Range.equal(DOUBLE, 200011.0),
                                Range.equal(DOUBLE, 200014.0),
                                Range.equal(DOUBLE, 200017.0),
                                Range.equal(DOUBLE, 200116.5),
                                Range.range(DOUBLE, 200030.0, true, 200036.0, true),
                                Range.range(DOUBLE, 200048.0, true, 200099.0, true))),
                        false))
                .put(columns.get(7), Domain.create(SortedRangeSet.copyOf(TINYINT,
                        ImmutableList.of(
                                Range.range(TINYINT, 60L, true, 70L, false),
                                Range.range(TINYINT, 52L, true, 55L, false))),
                        false))
                .put(columns.get(8), Domain.create(SortedRangeSet.copyOf(SMALLINT,
                        ImmutableList.of(
                                Range.range(SMALLINT, -75L, true, -68L, true),
                                Range.range(SMALLINT, -200L, true, -100L, false))),
                        false))
                .put(columns.get(9), Domain.create(SortedRangeSet.copyOf(INTEGER,
                        ImmutableList.of(
                                Range.equal(INTEGER, 80L),
                                Range.equal(INTEGER, 96L),
                                Range.lessThan(INTEGER, 0L))),
                        false))
                .put(columns.get(2), Domain.create(SortedRangeSet.copyOf(BOOLEAN,
                        ImmutableList.of(Range.equal(BOOLEAN, true))),
                        false))
                .buildOrThrow());

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE (\"col_0\" < ? OR (\"col_0\" >= ? AND \"col_0\" <= ?) OR \"col_0\" > ? OR \"col_0\" IN (?,?)) " +
                    "AND ((\"col_1\" >= ? AND \"col_1\" <= ?) OR (\"col_1\" >= ? AND \"col_1\" <= ?) OR \"col_1\" IN (?,?,?,?)) " +
                    "AND ((\"col_7\" >= ? AND \"col_7\" < ?) OR (\"col_7\" >= ? AND \"col_7\" < ?)) " +
                    "AND ((\"col_8\" >= ? AND \"col_8\" < ?) OR (\"col_8\" >= ? AND \"col_8\" <= ?)) " +
                    "AND (\"col_9\" < ? OR \"col_9\" IN (?,?)) " +
                    "AND \"col_2\" = ?");
            ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    builder.add((Long) resultSet.getObject("col_0"));
                }
            }
            assertEquals(builder.build(), ImmutableSet.of(68L, 180L, 196L));
        }
    }

    /**
     * Test query generation for domains originating from {@code NOT IN} predicates.
     *
     * @see Domain#complement()
     */
    @Test
    public void testBuildSqlWithDomainComplement()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                // complement of a Domain with null not allowed
                .put(columns.get(0), Domain.create(ValueSet.of(BIGINT, 128L, 180L, 233L), false).complement())
                // complement of a Domain with null allowed
                .put(columns.get(1), Domain.create(ValueSet.of(DOUBLE, 200011.0, 200014.0, 200017.0), true).complement())
                // this is here only to limit the list of results being read
                .put(columns.get(9), Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 880L)), false))
                .buildOrThrow());

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(
                jdbcClient,
                SESSION,
                connection,
                TEST_TABLE,
                Optional.empty(),
                List.of(columns.get(0), columns.get(3), columns.get(9)),
                Map.of(),
                tupleDomain,
                Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_3\", \"col_9\" " +
                    "FROM \"test_table\" " +
                    "WHERE (NOT (\"col_0\" IN (?,?,?)) OR \"col_0\" IS NULL) " +
                    "AND NOT (\"col_1\" IN (?,?,?)) " +
                    "AND \"col_9\" >= ?");
            ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    builder.add((Long) resultSet.getObject("col_0"));
                }
            }
            assertEquals(builder.build(), LongStream.range(980, 1000).boxed().collect(toImmutableList()));
        }
    }

    @Test
    public void testBuildSqlWithFloat()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(10), Domain.create(SortedRangeSet.copyOf(REAL,
                        ImmutableList.of(
                                Range.equal(REAL, (long) floatToRawIntBits(100.0f + 0)),
                                Range.equal(REAL, (long) floatToRawIntBits(100.008f + 0)),
                                Range.equal(REAL, (long) floatToRawIntBits(100.0f + 14)))),
                        false)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE \"col_10\" IN (?,?,?)");
            ImmutableSet.Builder<Long> longBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Float> floatBuilder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    longBuilder.add((Long) resultSet.getObject("col_0"));
                    floatBuilder.add((Float) resultSet.getObject("col_10"));
                }
            }
            assertEquals(longBuilder.build(), ImmutableSet.of(0L, 14L));
            assertEquals(floatBuilder.build(), ImmutableSet.of(100.0f, 114.0f));
        }
    }

    @Test
    public void testBuildSqlWithVarchar()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(3), Domain.create(SortedRangeSet.copyOf(VARCHAR,
                        ImmutableList.of(
                                Range.range(VARCHAR, utf8Slice("test_str_700"), true, utf8Slice("test_str_702"), false),
                                Range.equal(VARCHAR, utf8Slice("test_str_180")),
                                Range.equal(VARCHAR, utf8Slice("test_str_196")))),
                        false)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE ((\"col_3\" >= ? AND \"col_3\" < ?) OR \"col_3\" IN (?,?))");
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    builder.add((String) resultSet.getObject("col_3"));
                }
            }
            assertEquals(builder.build(), ImmutableSet.of("test_str_700", "test_str_701", "test_str_180", "test_str_196"));

            assertContains(preparedStatement.toString(), "\"col_3\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_3\" < ?");
            assertContains(preparedStatement.toString(), "\"col_3\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithChar()
            throws SQLException
    {
        CharType charType = CharType.createCharType(0);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(11), Domain.create(SortedRangeSet.copyOf(charType,
                        ImmutableList.of(
                                Range.range(charType, utf8Slice("test_str_700"), true, utf8Slice("test_str_702"), false),
                                Range.equal(charType, utf8Slice("test_str_180")),
                                Range.equal(charType, utf8Slice("test_str_196")))),
                        false)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE ((\"col_11\" >= ? AND \"col_11\" < ?) OR \"col_11\" IN (?,?))");
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    builder.add((String) resultSet.getObject("col_11"));
                }
            }
            assertEquals(builder.build(), ImmutableSet.of("test_str_700", "test_str_701", "test_str_180", "test_str_196"));

            assertContains(preparedStatement.toString(), "\"col_11\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_11\" < ?");
            assertContains(preparedStatement.toString(), "\"col_11\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithDateTime()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(4), Domain.create(SortedRangeSet.copyOf(DATE,
                        ImmutableList.of(
                                Range.range(DATE, toDays(2016, 6, 7), true, toDays(2016, 6, 17), false),
                                Range.equal(DATE, toDays(2016, 6, 3)),
                                Range.equal(DATE, toDays(2016, 10, 21)))),
                        false),
                columns.get(5), Domain.create(SortedRangeSet.copyOf(TIME,
                        ImmutableList.of(
                                Range.range(TIME, toTimeRepresentation(6, 12, 23), false, toTimeRepresentation(8, 23, 37), true),
                                Range.equal(TIME, toTimeRepresentation(2, 3, 4)),
                                Range.equal(TIME, toTimeRepresentation(20, 23, 37)))),
                        false)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE ((\"col_4\" >= ? AND \"col_4\" < ?) OR \"col_4\" IN (?,?)) AND ((\"col_5\" > ? AND \"col_5\" <= ?) OR \"col_5\" IN (?,?))");
            ImmutableSet.Builder<Date> dateBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Time> timeBuilder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    dateBuilder.add((Date) resultSet.getObject("col_4"));
                    timeBuilder.add((Time) resultSet.getObject("col_5"));
                }
            }
            assertEquals(dateBuilder.build(), ImmutableSet.of(toDate(2016, 6, 7), toDate(2016, 6, 13), toDate(2016, 10, 21)));
            assertEquals(timeBuilder.build(), ImmutableSet.of(toTime(8, 23, 37), toTime(20, 23, 37)));

            assertContains(preparedStatement.toString(), "\"col_4\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_4\" < ?");
            assertContains(preparedStatement.toString(), "\"col_4\" IN (?,?)");
            assertContains(preparedStatement.toString(), "\"col_5\" > ?");
            assertContains(preparedStatement.toString(), "\"col_5\" <= ?");
            assertContains(preparedStatement.toString(), "\"col_5\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithTimestamp()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(6), Domain.create(SortedRangeSet.copyOf(TIMESTAMP_MILLIS,
                        ImmutableList.of(
                                Range.equal(TIMESTAMP_MILLIS, toPrestoTimestamp(2016, 6, 3, 0, 23, 37)),
                                Range.equal(TIMESTAMP_MILLIS, toPrestoTimestamp(2016, 10, 19, 16, 23, 37)),
                                Range.range(TIMESTAMP_MILLIS, toPrestoTimestamp(2016, 6, 7, 8, 23, 37), false, toPrestoTimestamp(2016, 6, 9, 12, 23, 37), true))),
                        false)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE ((\"col_6\" > ? AND \"col_6\" <= ?) OR \"col_6\" IN (?,?))");
            ImmutableSet.Builder<Timestamp> builder = ImmutableSet.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    builder.add((Timestamp) resultSet.getObject("col_6"));
                }
            }
            assertEquals(builder.build(), ImmutableSet.of(
                    toTimestamp(2016, 6, 3, 0, 23, 37),
                    toTimestamp(2016, 6, 8, 10, 23, 37),
                    toTimestamp(2016, 6, 9, 12, 23, 37),
                    toTimestamp(2016, 10, 19, 16, 23, 37)));

            assertContains(preparedStatement.toString(), "\"col_6\" > ?");
            assertContains(preparedStatement.toString(), "\"col_6\" <= ?");
            assertContains(preparedStatement.toString(), "\"col_6\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithLimit()
            throws SQLException
    {
        Connection connection = database.getConnection();
        Function<String, String> function = sql -> sql + " LIMIT 10";
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), TupleDomain.all(), Optional.empty());
        preparedQuery = preparedQuery.transformQuery(function);
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "LIMIT 10");
            long count = 0;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    count++;
                }
            }
            assertEquals(count, 10);
        }
    }

    @Test
    public void testEmptyBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(0), Domain.all(BIGINT),
                columns.get(1), Domain.onlyNull(DOUBLE)));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(jdbcClient, SESSION, connection, TEST_TABLE, Optional.empty(), columns, Map.of(), tupleDomain, Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", \"col_4\", \"col_5\", " +
                    "\"col_6\", \"col_7\", \"col_8\", \"col_9\", \"col_10\", \"col_11\" " +
                    "FROM \"test_table\" " +
                    "WHERE \"col_1\" IS NULL");
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertEquals(resultSet.next(), false);
            }
        }
    }

    @Test
    public void testAggregation()
            throws SQLException
    {
        List<JdbcColumnHandle> projectedColumns = ImmutableList.of(
                this.columns.get(2),
                new JdbcColumnHandle(
                        "s",
                        JDBC_BIGINT,
                        BIGINT,
                        true,
                        Optional.empty()));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(
                jdbcClient,
                SESSION,
                connection,
                TEST_TABLE,
                Optional.of(ImmutableList.of(ImmutableList.of(this.columns.get(2)))),
                projectedColumns,
                Map.of("s", "sum(\"col_0\")"),
                TupleDomain.all(),
                Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_2\", sum(\"col_0\") AS \"s\" " +
                    "FROM \"test_table\" " +
                    "GROUP BY \"col_2\"");

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                Multiset<List<Object>> actual = read(resultSet);
                assertThat(actual)
                        .isEqualTo(ImmutableMultiset.of(
                                ImmutableList.of(false, BigDecimal.valueOf(250000)),
                                ImmutableList.of(true, BigDecimal.valueOf(249500))));
            }
        }
    }

    @Test
    public void testAggregationWithFilter()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                this.columns.get(1), Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 200042.0)), true)));

        List<JdbcColumnHandle> projectedColumns = ImmutableList.of(
                this.columns.get(2),
                new JdbcColumnHandle(
                        "s",
                        JDBC_BIGINT,
                        BIGINT,
                        true,
                        Optional.empty()));

        Connection connection = database.getConnection();
        QueryBuilder queryBuilder = new QueryBuilder(jdbcClient);
        PreparedQuery preparedQuery = queryBuilder.prepareQuery(
                jdbcClient,
                SESSION,
                connection,
                TEST_TABLE,
                Optional.of(ImmutableList.of(ImmutableList.of(this.columns.get(2)))),
                projectedColumns,
                Map.of("s", "sum(\"col_0\")"),
                tupleDomain,
                Optional.empty());
        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(jdbcClient, SESSION, connection, preparedQuery)) {
            assertThat(preparedQuery.getQuery()).isEqualTo("" +
                    "SELECT \"col_2\", sum(\"col_0\") AS \"s\" " +
                    "FROM \"test_table\" " +
                    "WHERE (\"col_1\" < ? OR \"col_1\" IS NULL) " +
                    "GROUP BY \"col_2\"");

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                Multiset<List<Object>> actual = read(resultSet);
                assertThat(actual)
                        .isEqualTo(ImmutableMultiset.of(
                                ImmutableList.of(false, BigDecimal.valueOf(1764)),
                                ImmutableList.of(true, BigDecimal.valueOf(1722))));
            }
        }
    }

    private static long toPrestoTimestamp(int year, int month, int day, int hour, int minute, int second)
    {
        return sqlTimestampOf(3, year, month, day, hour, minute, second, 0).getMillis() * MICROSECONDS_PER_MILLISECOND;
    }

    private static Timestamp toTimestamp(int year, int month, int day, int hour, int minute, int second)
    {
        return Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute, second));
    }

    private static long toDays(int year, int month, int day)
    {
        return DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(year, month, day));
    }

    private static Date toDate(int year, int month, int day)
    {
        return Date.valueOf(format("%d-%d-%d", year, month, day));
    }

    private static Time toTime(int hour, int minute, int second)
    {
        return Time.valueOf(LocalTime.of(hour, minute, second));
    }

    private static long toTimeRepresentation(int hour, int minute, int second)
    {
        SqlTime time = sqlTimeOf(hour, minute, second, 0);
        return time.getPicos();
    }

    private static Multiset<List<Object>> read(ResultSet resultSet)
            throws SQLException
    {
        ImmutableMultiset.Builder<List<Object>> result = ImmutableMultiset.builder();
        while (resultSet.next()) {
            ImmutableList.Builder<Object> row = ImmutableList.builder();
            for (int column = 1; column <= resultSet.getMetaData().getColumnCount(); column++) {
                row.add(resultSet.getObject(column));
            }
            result.add(row.build());
        }
        return result.build();
    }
}
