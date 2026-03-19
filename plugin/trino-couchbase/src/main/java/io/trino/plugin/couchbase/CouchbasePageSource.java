package io.trino.plugin.couchbase;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static com.google.common.base.Verify.verify;

public final class CouchbasePageSource implements ConnectorPageSource {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbasePageSource.class);
    private final ConnectorTransactionHandle transaction;
    private final ConnectorSession session;
    private final ConnectorSplit split;
    private final CouchbaseTableHandle table;
    private final PageBuilder pageBuilder;
    private final CouchbaseClient client;
    private final String queryString;
    private final Long pageSize;
    private long offset = 0;
    private boolean finished = false;

    public CouchbasePageSource(CouchbaseClient client, CouchbaseTransactionHandle transaction, ConnectorSession session,
                               CouchbaseSplit split, CouchbaseTableHandle table, List<CouchbaseColumnHandle> columns,
                               DynamicFilter dynamicFilter, Long pageSize) {
        this.client = client;
        this.transaction = transaction;
        this.session = session;
        this.split = split;
        this.pageSize = pageSize;

        if (columns != null && !columns.isEmpty()) {
            table.addColumns(columns);
            table = table.wrap();
            table.addColumns(columns);
        }

        this.table = table;

        TupleDomain<ColumnHandle> predicate = dynamicFilter.getCurrentPredicate();

        if (!predicate.isAll()) {
            table.addPredicate(predicate);
        }

        queryString = table.toSql();
        this.pageBuilder = new PageBuilder(table.selectTypes().stream().toList());
    }


    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        verify(pageBuilder.isEmpty());
        JsonArray queryArgs = JsonArray.create();
        table.getParameters().forEach(queryArgs::add);
        QueryOptions options = QueryOptions.queryOptions()
                .parameters(queryArgs);

        final String query = String.format("SELECT data.* FROM (%s) data OFFSET %d LIMIT %d", queryString, offset, pageSize);
        QueryResult result = client.getScope().query(query, options);
        List<JsonObject> rows = result.rowsAsObject();
        LOG.info("Couchbase query ({} result rows): {}; arguments: {}", rows.size(), query, queryArgs);
        final List<Type> types = table.selectTypes();
        final List<String> names = table.selectNames();

        for (int j = 0; j < rows.size(); j++) {
            JsonObject row = rows.get(j);
            pageBuilder.declarePosition();
            for (int i = 0; i < table.selectClauses().size(); i++) {
                Type type = types.get(i);
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendValue(output, type, row.get(names.get(i)));
            }
        }
        offset += pageSize;

        if (rows.size() != pageSize) {
            finished = true;
        } else if (rows.isEmpty()) {
            finished = true;
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    private void appendValue(BlockBuilder output, Type type, Object value) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();

        try {
            if (type == BooleanType.BOOLEAN) {
                type.writeBoolean(output, Boolean.valueOf(String.valueOf(value)));
                return;
            } else if (type == VarcharType.VARCHAR || javaType == Slice.class) {
                Slice slice = Slices.utf8Slice(String.valueOf(value));
                type.writeSlice(output, slice);
            } else if (javaType == long.class) {
                if (type.equals(BigintType.BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                } else if (type.equals(IntegerType.INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                } else if (type.equals(SmallintType.SMALLINT)) {
                    type.writeLong(output, Shorts.checkedCast(((Number) value).longValue()));
                } else if (type.equals(TinyintType.TINYINT)) {
                    type.writeLong(output, SignedBytes.checkedCast(((Number) value).longValue()));
                } else if (type.equals(RealType.REAL)) {
                    type.writeLong(output, Float.floatToIntBits(((Number) value).floatValue()));
                } else if (type instanceof DecimalType decimalType) {
                    LOG.info("test");
                    throw new RuntimeException("test");
//                    Decimal128 decimal = (Decimal128) value;
//                    if (decimal.compareTo(Decimal128.) == 0) {
//                        type.writeLong(output, encodeShortScaledValue(BigDecimal.ZERO, decimalType.getScale()));
//                    }
//                    else {
//                        type.writeLong(output, encodeShortScaledValue(decimal.bigDecimalValue(), decimalType.getScale()));
//                    }
                } else if (type.equals(DateType.DATE)) {
                    type.writeLong(output, Long.valueOf(value.toString()));
                } else {
                    throw new RuntimeException("Unsupported type: " + type);
                }
            } else if (javaType == Int128.class) {
                DecimalType decimalType = (DecimalType) type;
                if (value instanceof Integer intValue) {
                    if (intValue == 0) {
                        type.writeObject(output, Decimals.encodeScaledValue(BigDecimal.ZERO, decimalType.getScale()));
                    } else {
                        type.writeObject(output, Decimals.encodeScaledValue(BigDecimal.valueOf(intValue), decimalType.getScale()));
                    }
                } else if (value instanceof Double doubleValue) {
                    if (doubleValue == 0.0d) {
                        type.writeObject(output, Decimals.encodeScaledValue(BigDecimal.ZERO, decimalType.getScale()));
                    } else {
                        BigDecimal result = new BigDecimal(BigInteger.valueOf(doubleValue.longValue()));
                        type.writeObject(output, Decimals.encodeScaledValue(result, decimalType.getScale()));
                    }
                } else {
                    throw new RuntimeException("Unsupported type: " + value.getClass());
                }
            } else if (javaType == Double.class || javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            } else {
                throw new RuntimeException("Unsupported type " + javaType);
            }
            return;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to append value '%s' of type %s from object type %s",
                            String.valueOf(value),
                            type,
                            value.getClass()
                    ),
                    e
            );
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    public ConnectorTransactionHandle transaction() {
        return transaction;
    }

    public ConnectorSession session() {
        return session;
    }

    public ConnectorSplit split() {
        return split;
    }

    public ConnectorTableHandle table() {
        return table;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CouchbasePageSource) obj;
        return Objects.equals(this.transaction, that.transaction) &&
                Objects.equals(this.session, that.session) &&
                Objects.equals(this.split, that.split) &&
                Objects.equals(this.table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, session, split, table);
    }

    @Override
    public String toString() {
        return "CouchbasePageSource[" +
                "transaction=" + transaction + ", " +
                "session=" + session + ", " +
                "split=" + split + ", " +
                "table=" + table + ']';
    }

}
