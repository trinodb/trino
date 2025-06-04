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
package io.trino.testing;

import com.google.common.base.Joiner;
import io.trino.Session;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.ParsedSql;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.SqlParser;
import org.jdbi.v3.core.statement.StatementContext;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JsonFunctions.jsonParse;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.plugin.tpch.TpchRecordSet.createTpchRecordSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.nCopies;

public class H2QueryRunner
        implements Closeable
{
    private final Handle handle;

    public H2QueryRunner()
    {
        handle = Jdbi.open("jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong() + ";NON_KEYWORDS=KEY,VALUE"); // key and value are reserved keywords in H2 2.x
        TpchMetadata tpchMetadata = new TpchMetadata();

        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus VARCHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate DATE NOT NULL,\n" +
                "  orderpriority VARCHAR(15) NOT NULL,\n" +
                "  clerk VARCHAR(15) NOT NULL,\n" +
                "  shippriority INTEGER NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        handle.execute("CREATE INDEX custkey_index ON orders (custkey)");
        insertRows(tpchMetadata, ORDERS);

        handle.execute("CREATE TABLE lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber INTEGER,\n" +
                "  quantity DOUBLE NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate DATE NOT NULL,\n" +
                "  commitdate DATE NOT NULL,\n" +
                "  receiptdate DATE NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        insertRows(tpchMetadata, LINE_ITEM);

        handle.execute("CREATE TABLE nation (\n" +
                "  nationkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  regionkey BIGINT NOT NULL,\n" +
                "  comment VARCHAR(114) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, NATION);

        handle.execute("CREATE TABLE region(\n" +
                "  regionkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  comment VARCHAR(115) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, REGION);

        handle.execute("CREATE TABLE part(\n" +
                "  partkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(55) NOT NULL,\n" +
                "  mfgr VARCHAR(25) NOT NULL,\n" +
                "  brand VARCHAR(10) NOT NULL,\n" +
                "  type VARCHAR(25) NOT NULL,\n" +
                "  size INTEGER NOT NULL,\n" +
                "  container VARCHAR(10) NOT NULL,\n" +
                "  retailprice DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(23) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, PART);

        handle.execute("CREATE TABLE customer (\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  address VARCHAR(40) NOT NULL,\n" +
                "  nationkey BIGINT NOT NULL,\n" +
                "  phone VARCHAR(15) NOT NULL,\n" +
                "  acctbal DOUBLE NOT NULL,\n" +
                "  mktsegment VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(117) NOT NULL,\n" +
                "  PRIMARY KEY (custkey)" +
                ")");
        insertRows(tpchMetadata, CUSTOMER);
    }

    private void insertRows(TpchMetadata tpchMetadata, TpchTable<?> tpchTable)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, tpchTable.getTableName()), Optional.empty(), Optional.empty());
        insertRows(tpchMetadata.getTableMetadata(null, tableHandle), handle, createTpchRecordSet(tpchTable, tableHandle.scaleFactor()));
    }

    @Override
    public void close()
    {
        handle.close();
    }

    public MaterializedResult execute(Session session, @Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        MaterializedResult materializedRows = new MaterializedResult(
                Optional.of(session),
                handle.setSqlParser(new RawSqlParser())
                        .setTemplateEngine((template, context) -> template)
                        .createQuery(sql)
                        .map(rowMapper(resultTypes))
                        .list(),
                resultTypes);

        return materializedRows;
    }

    private static RowMapper<MaterializedRow> rowMapper(List<? extends Type> types)
    {
        return (resultSet, context) -> {
            int count = resultSet.getMetaData().getColumnCount();
            checkArgument(types.size() == count, "expected types count (%s) does not match actual column count (%s)", types.size(), count);
            List<Object> row = new ArrayList<>(count);
            for (int i = 1; i <= count; i++) {
                Type type = types.get(i - 1);
                if (BOOLEAN.equals(type)) {
                    boolean booleanValue = resultSet.getBoolean(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(booleanValue);
                    }
                }
                else if (TINYINT.equals(type)) {
                    byte byteValue = resultSet.getByte(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(byteValue);
                    }
                }
                else if (SMALLINT.equals(type)) {
                    short shortValue = resultSet.getShort(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(shortValue);
                    }
                }
                else if (INTEGER.equals(type)) {
                    int intValue = resultSet.getInt(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(intValue);
                    }
                }
                else if (BIGINT.equals(type)) {
                    long longValue = resultSet.getLong(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(longValue);
                    }
                }
                else if (REAL.equals(type)) {
                    float floatValue = resultSet.getFloat(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(floatValue);
                    }
                }
                else if (DOUBLE.equals(type)) {
                    double doubleValue = resultSet.getDouble(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(doubleValue);
                    }
                }
                else if (JSON.equals(type)) {
                    String stringValue = resultSet.getString(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(jsonParse(utf8Slice(stringValue)).toStringUtf8());
                    }
                }
                else if (type instanceof VarcharType || type instanceof CharType) {
                    String stringValue = resultSet.getString(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(stringValue);
                    }
                }
                else if (VARBINARY.equals(type)) {
                    byte[] bytes = resultSet.getBytes(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(bytes);
                    }
                }
                else if (DATE.equals(type)) {
                    // resultSet.getDate(i) doesn't work if JVM's zone skipped day being retrieved (e.g. 2011-12-30 and Pacific/Apia zone)
                    LocalDate dateValue = resultSet.getObject(i, LocalDate.class);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(dateValue);
                    }
                }
                else if (type instanceof TimeType) {
                    // resultSet.getTime(i) doesn't work if JVM's zone had forward offset change during 1970-01-01 (e.g. America/Hermosillo zone)
                    LocalTime timeValue = resultSet.getObject(i, LocalTime.class);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(timeValue);
                    }
                }
                else if (type instanceof TimeWithTimeZoneType) {
                    throw new UnsupportedOperationException("H2 does not support TIME WITH TIME ZONE");
                }
                else if (type instanceof TimestampType) {
                    // resultSet.getTimestamp(i) doesn't work if JVM's zone had forward offset at the date/time being retrieved
                    LocalDateTime timestampValue;
                    try {
                        timestampValue = resultSet.getObject(i, LocalDateTime.class);
                    }
                    catch (SQLException first) {
                        // H2 cannot convert DATE to LocalDateTime in their JDBC driver (even though it can convert to java.sql.Timestamp), we need to do this manually
                        try {
                            timestampValue = Optional.ofNullable(resultSet.getObject(i, LocalDate.class)).map(LocalDate::atStartOfDay).orElse(null);
                        }
                        catch (RuntimeException e) {
                            first.addSuppressed(e);
                            throw first;
                        }
                    }
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(timestampValue);
                    }
                }
                else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
                    // H2 supports TIMESTAMP WITH TIME ZONE via org.h2.api.TimestampWithTimeZone, but it represent only a fixed-offset TZ (not named)
                    // This means H2 is unsuitable for testing TIMESTAMP WITH TIME ZONE-bearing queries. Those need to be tested manually.
                    throw new UnsupportedOperationException();
                }
                else if (UUID.equals(type)) {
                    java.util.UUID value = (java.util.UUID) resultSet.getObject(i);
                    row.add(value);
                }
                else if (UNKNOWN.equals(type)) {
                    Object objectValue = resultSet.getObject(i);
                    checkState(resultSet.wasNull(), "Expected a null value, but got %s", objectValue);
                    row.add(null);
                }
                else if (type instanceof DecimalType decimalType) {
                    BigDecimal decimalValue = resultSet.getBigDecimal(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(decimalValue
                                .setScale(decimalType.getScale(), HALF_UP)
                                .round(new MathContext(decimalType.getPrecision())));
                    }
                }
                else if (type instanceof ArrayType) {
                    Array array = resultSet.getArray(i);
                    if (resultSet.wasNull()) {
                        row.add(null);
                    }
                    else {
                        row.add(newArrayList((Object[]) array.getArray()));
                    }
                }
                else {
                    throw new AssertionError("unhandled type: " + type);
                }
            }
            return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, row);
        };
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toImmutableList());

        String vars = Joiner.on(',').join(nCopies(columns.size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableMetadata.getTable().getTableName(), vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    if (batch.size() > 0) {
                        batch.execute();
                    }
                    return;
                }
                for (int column = 0; column < columns.size(); column++) {
                    Type type = columns.get(column).getType();
                    if (BOOLEAN.equals(type)) {
                        batch.bind(column, cursor.getBoolean(column));
                    }
                    else if (BIGINT.equals(type)) {
                        batch.bind(column, cursor.getLong(column));
                    }
                    else if (INTEGER.equals(type)) {
                        batch.bind(column, toIntExact(cursor.getLong(column)));
                    }
                    else if (DOUBLE.equals(type)) {
                        batch.bind(column, cursor.getDouble(column));
                    }
                    else if (type instanceof VarcharType) {
                        batch.bind(column, cursor.getSlice(column).toStringUtf8());
                    }
                    else if (DATE.equals(type)) {
                        long millisUtc = TimeUnit.DAYS.toMillis(cursor.getLong(column));
                        // H2 expects dates in to be millis at midnight in the JVM timezone
                        long localMillis = DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millisUtc);
                        batch.bind(column, new Date(localMillis));
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported type " + type);
                    }
                }
                batch.add();
            }
            batch.execute();
        }
    }

    /**
     * Pass-through SQL parser that does not support named parameters or definitions.
     * This allows queries such as {@code x<y} that do not work with the default parser.
     */
    private static class RawSqlParser
            implements SqlParser
    {
        @Override
        public ParsedSql parse(String sql, StatementContext ctx)
        {
            return ParsedSql.builder().append(sql).build();
        }

        @Override
        public String nameParameter(String rawName, StatementContext ctx)
        {
            throw new UnsupportedOperationException();
        }
    }
}
