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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.plugin.redis.decoder.RedisRowDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.FieldValueProviders.booleanValueProvider;
import static io.trino.decoder.FieldValueProviders.bytesValueProvider;
import static io.trino.decoder.FieldValueProviders.longValueProvider;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

public class RedisRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(RedisRecordCursor.class);
    private static final String EMPTY_STRING = "";

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final RedisSplit split;
    private final List<RedisColumnHandle> columnHandles;
    private final JedisPool jedisPool;
    private final ScanParams scanParams;
    private final int maxKeysPerFetch;
    private final char redisKeyDelimiter;
    private final boolean isKeyPrefixSchemaTable;
    private final int redisScanCount;

    private ScanResult<String> redisCursor;
    private List<String> keys;

    private final AtomicBoolean reported = new AtomicBoolean();

    private List<String> stringValues;
    private List<Object> hashValues;

    private long totalBytes;
    private long totalValues;

    private final Queue<FieldValueProvider[]> currentRowGroup;

    RedisRecordCursor(
            RowDecoder keyDecoder,
            RowDecoder valueDecoder,
            RedisSplit split,
            List<RedisColumnHandle> columnHandles,
            RedisJedisManager redisJedisManager)
    {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.split = split;
        this.columnHandles = columnHandles;
        this.jedisPool = redisJedisManager.getJedisPool(split.getNodes().get(0));
        this.redisKeyDelimiter = redisJedisManager.getRedisKeyDelimiter();
        this.isKeyPrefixSchemaTable = redisJedisManager.isKeyPrefixSchemaTable();
        this.redisScanCount = redisJedisManager.getRedisScanCount();
        this.scanParams = setScanParams();
        this.maxKeysPerFetch = redisJedisManager.getRedisMaxKeysPerFetch();
        this.currentRowGroup = new LinkedList<>();

        if (split.getConstraint().isAll()) {
            fetchKeys();
        }
        else {
            setPushdownKeys();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    public boolean hasUnscannedData()
    {
        if (redisCursor == null) {
            return false;
        }
        // no more keys are unscanned when
        // when redis scan command
        // returns 0 string cursor
        return (!redisCursor.getCursor().equals("0"));
    }

    @Override
    public boolean advanceNextPosition()
    {
        // When the row of data is processed, it needs to be removed from the queue
        currentRowGroup.poll();
        while (currentRowGroup.isEmpty()) {
            while (keys.isEmpty()) {
                if (!split.getConstraint().isAll()) {
                    return false;
                }
                if (!hasUnscannedData()) {
                    return endOfData();
                }
                fetchKeys();
            }
            fetchNextRowGroup();
        }
        return true;
    }

    private boolean endOfData()
    {
        if (!reported.getAndSet(true)) {
            log.debug("Read a total of %d values with %d bytes.", totalValues, totalBytes);
        }
        return false;
    }

    private void fetchNextRowGroup()
    {
        List<String> currentKeys = keys.size() > maxKeysPerFetch ? keys.subList(0, maxKeysPerFetch) : keys;
        fetchData(currentKeys);

        switch (split.getValueDataType()) {
            case STRING:
                processStringValues(currentKeys);
                break;
            case HASH:
                processHashValues(currentKeys);
                break;
            default:
                log.warn("Redis value of type %s is unsupported", split.getValueDataType());
        }
        currentKeys.clear();
    }

    private void processStringValues(List<String> currentKeys)
    {
        for (int i = 0; i < currentKeys.size(); i++) {
            String keyString = currentKeys.get(i);
            // If the value corresponding to the key does not exist, the valueString is null
            String valueString = stringValues.get(i);
            if (valueString == null) {
                log.warn("The string value at key %s does not exist", keyString);
                continue;
            }
            generateRowValues(keyString, valueString, null);
        }
    }

    private void processHashValues(List<String> currentKeys)
    {
        for (int i = 0; i < currentKeys.size(); i++) {
            String keyString = currentKeys.get(i);
            Object object = hashValues.get(i);
            if (object instanceof JedisDataException) {
                throw (JedisDataException) object;
            }
            Map<String, String> hashValueMap = (Map<String, String>) object;
            if (hashValueMap.isEmpty()) {
                log.warn("The hash value at key %s does not exist", keyString);
                continue;
            }
            generateRowValues(keyString, EMPTY_STRING, hashValueMap);
        }
    }

    private void generateRowValues(String keyString, String valueString, @Nullable Map<String, String> hashValueMap)
    {
        byte[] keyData = keyString.getBytes(StandardCharsets.UTF_8);
        byte[] stringValueData = valueString.getBytes(StandardCharsets.UTF_8);
        // Redis connector supports two types of Redis values: STRING and HASH. HASH type requires hash row decoder to
        // decode a row from map, whereas for the STRING type decoders are optional. The redis keyData is always byte array,
        // so the decoder of key always decodes a row from bytes.
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = valueDecoder instanceof RedisRowDecoder
                ? ((RedisRowDecoder) valueDecoder).decodeRow(hashValueMap)
                : valueDecoder.decodeRow(stringValueData);

        totalBytes += stringValueData.length;
        totalValues++;

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                RedisInternalFieldDescription fieldDescription = RedisInternalFieldDescription.forColumnName(columnHandle.getName());
                switch (fieldDescription) {
                    case KEY_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                        break;
                    case VALUE_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(stringValueData));
                        break;
                    case KEY_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                        break;
                    case VALUE_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(stringValueData.length));
                        break;
                    case KEY_CORRUPT_FIELD:
                        currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedKey.isEmpty()));
                        break;
                    case VALUE_CORRUPT_FIELD:
                        currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedValue.isEmpty()));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                }
            }
        }

        decodedKey.ifPresent(currentRowValuesMap::putAll);
        decodedValue.ifPresent(currentRowValuesMap::putAll);

        FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }
        currentRowGroup.offer(currentRowValues);
    }

    @Override
    public boolean getBoolean(int field)
    {
        return getFieldValueProvider(field, boolean.class).getBoolean();
    }

    @Override
    public long getLong(int field)
    {
        return getFieldValueProvider(field, long.class).getLong();
    }

    @Override
    public double getDouble(int field)
    {
        return getFieldValueProvider(field, double.class).getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        return getFieldValueProvider(field, Slice.class).getSlice();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        FieldValueProvider[] currentRowValues = currentRowGroup.peek();
        return currentRowValues == null || currentRowValues[field].isNull();
    }

    @Override
    public Object getObject(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        throw new IllegalArgumentException(format("Type %s is not supported", getType(field)));
    }

    private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        checkFieldType(field, expectedType);
        FieldValueProvider[] currentRowValues = currentRowGroup.peek();
        return requireNonNull(currentRowValues)[field];
    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }

    private ScanParams setScanParams()
    {
        if (split.getKeyDataType() == RedisDataType.STRING) {
            ScanParams scanParams = new ScanParams();
            scanParams.count(redisScanCount);

            // when Redis key string follows "schema:table:*" format
            // scan command can efficiently query tables
            // by returning matching keys
            // the alternative is to set key-prefix-schema-table to false
            // and treat entire redis as single schema , single table
            // redis Hash/Set types are to be supported - they can also be
            // used to filter out table data

            // "default" schema is not prefixed to the key

            if (isKeyPrefixSchemaTable) {
                String keyMatch = "";
                if (!split.getSchemaName().equals("default")) {
                    keyMatch = split.getSchemaName() + redisKeyDelimiter;
                }
                keyMatch = keyMatch + split.getTableName() + redisKeyDelimiter + "*";
                scanParams.match(keyMatch);
            }
            return scanParams;
        }

        return null;
    }

    private void setPushdownKeys()
    {
        String keyStringPrefix = isKeyPrefixSchemaTable
                ? scanParams.match().substring(0, scanParams.match().length() - 1)
                : EMPTY_STRING;
        TupleDomain<ColumnHandle> constraint = split.getConstraint();
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow();

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            if (((RedisColumnHandle) entry.getKey()).isKeyDecoder()) {
                Domain domain = entry.getValue();
                if (domain.isSingleValue()) {
                    String value = ((Slice) domain.getSingleValue()).toStringUtf8();
                    keys = keyStringPrefix.isEmpty() || value.contains(keyStringPrefix) ? Lists.newArrayList(value) : emptyList();
                    log.debug("Set pushdown keys %s with single value", keys.toString());
                    return;
                }
                ValueSet valueSet = domain.getValues();
                if (valueSet instanceof SortedRangeSet) {
                    Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                    List<Range> rangeList = ranges.getOrderedRanges();
                    if (rangeList.stream().allMatch(Range::isSingleValue)) {
                        keys = rangeList.stream()
                                .map(range -> ((Slice) range.getSingleValue()).toStringUtf8())
                                .filter(str -> keyStringPrefix.isEmpty() || str.contains(keyStringPrefix))
                                .collect(toList());
                        log.debug("Set pushdown keys %s with sorted range values", keys.toString());
                        return;
                    }
                }
            }
        }
        keys = ImmutableList.of();
    }

    // Redis keys can be contained in the user-provided ZSET
    // Otherwise they need to be found by scanning Redis
    private void fetchKeys()
    {
        try (Jedis jedis = jedisPool.getResource()) {
            switch (split.getKeyDataType()) {
                case STRING: {
                    String cursor = SCAN_POINTER_START;
                    if (redisCursor != null) {
                        cursor = redisCursor.getCursor();
                    }

                    log.debug("Scanning new Redis keys from cursor %s . %d values read so far", cursor, totalValues);

                    redisCursor = jedis.scan(cursor, scanParams);
                    keys = redisCursor.getResult();
                }
                break;
                case ZSET:
                    keys = jedis.zrange(split.getKeyName(), split.getStart(), split.getEnd());
                    break;
                default:
                    log.warn("Redis key of type %s is unsupported", split.getKeyDataFormat());
            }
        }
    }

    private void fetchData(List<String> currentKeys)
    {
        stringValues = null;
        hashValues = null;
        try (Jedis jedis = jedisPool.getResource()) {
            switch (split.getValueDataType()) {
                case STRING:
                    stringValues = jedis.mget(currentKeys.toArray(new String[0]));
                    break;
                case HASH:
                    Pipeline pipeline = jedis.pipelined();
                    for (String key : currentKeys) {
                        pipeline.hgetAll(key);
                    }
                    hashValues = pipeline.syncAndReturnAll();
                    break;
                default:
                    log.warn("Redis value of type %s is unsupported", split.getValueDataType());
            }
        }
    }
}
