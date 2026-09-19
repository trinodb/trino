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
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.FieldValueProviders.booleanValueProvider;
import static io.trino.decoder.FieldValueProviders.bytesValueProvider;
import static io.trino.decoder.FieldValueProviders.longValueProvider;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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
    private static final int MAX_REDIRECTION_RETRIES = 5;

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final RedisSplit split;
    private final List<RedisColumnHandle> columnHandles;

    private final RedisClient client;
    private final RedisClientManager clientManager;
    private final ScanParams scanParams;
    private final int maxKeysPerFetch;
    private final char keyDelimiter;
    private final boolean isKeyPrefixSchemaTable;
    private final int scanCount;
    private final boolean clusterEnabled;

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
            RedisClientManager clientManager)
    {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.split = split;
        this.columnHandles = columnHandles;

        this.clientManager = clientManager;
        this.client = clientManager.getClient(split.getNodes().get(0));
        this.keyDelimiter = clientManager.getRedisKeyDelimiter();
        this.isKeyPrefixSchemaTable = clientManager.isKeyPrefixSchemaTable();
        this.scanCount = clientManager.getRedisScanCount();
        this.clusterEnabled = clientManager.isClusterEnabled();
        this.scanParams = setScanParams();
        this.maxKeysPerFetch = clientManager.getRedisMaxKeysPerFetch();
        this.currentRowGroup = new LinkedList<>();

        if (split.getClusterKeysOptional().isPresent()) {
            // Predicate-routed split: keys are pre-assigned to this primary by slot
            keys = new ArrayList<>(split.getClusterKeysOptional().get());
        }
        else if (split.getConstraint().isAll()) {
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
        // no more keys are unscanned
        // when redis scan command
        // returns 0 string cursor
        return !redisCursor.getCursor().equals("0");
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
            case STRING -> processStringValues(currentKeys);
            case HASH -> processHashValues(currentKeys);
            default -> log.warn("Redis value of type %s is unsupported", split.getValueDataType());
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
            if (object instanceof JedisDataException jedisDataException) {
                // Redirections should have been handled in fetchData with retry.
                // If we get here, it's a non-redirection error.
                throw jedisDataException;
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
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = valueDecoder instanceof RedisRowDecoder redisRowDecoder
                ? redisRowDecoder.decodeRow(hashValueMap)
                : valueDecoder.decodeRow(stringValueData);

        totalBytes += stringValueData.length;
        totalValues++;

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                RedisInternalFieldDescription fieldDescription = RedisInternalFieldDescription.forColumnName(columnHandle.getName());
                switch (fieldDescription) {
                    case KEY_FIELD -> currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                    case VALUE_FIELD -> currentRowValuesMap.put(columnHandle, bytesValueProvider(stringValueData));
                    case KEY_LENGTH_FIELD -> currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                    case VALUE_LENGTH_FIELD -> currentRowValuesMap.put(columnHandle, longValueProvider(stringValueData.length));
                    case KEY_CORRUPT_FIELD -> currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedKey.isEmpty()));
                    case VALUE_CORRUPT_FIELD -> currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedValue.isEmpty()));
                    default -> throw new IllegalArgumentException("unknown internal field " + fieldDescription);
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
    public void close() {}

    private static boolean isRedirectionError(JedisDataException exception)
    {
        return RedisClientManager.isRedirectionError(exception);
    }

    private ScanParams setScanParams()
    {
        if (split.getKeyDataType() == RedisDataType.STRING) {
            ScanParams scanParams = new ScanParams();
            scanParams.count(scanCount);

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
                    keyMatch = split.getSchemaName() + keyDelimiter;
                }
                keyMatch = keyMatch + split.getTableName() + keyDelimiter + "*";
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

        for (Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            if (((RedisColumnHandle) entry.getKey()).isKeyDecoder()) {
                Domain domain = entry.getValue();
                if (domain.isSingleValue()) {
                    String value = ((Slice) domain.getSingleValue()).toStringUtf8();
                    keys = keyStringPrefix.isEmpty() || value.contains(keyStringPrefix) ? Lists.newArrayList(value) : emptyList();
                    log.debug("Set pushdown keys %s with single value", keys.toString());
                    return;
                }
                ValueSet valueSet = domain.getValues();
                if (valueSet instanceof SortedRangeSet sortedRangeSet) {
                    Ranges ranges = sortedRangeSet.getRanges();
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
        switch (split.getKeyDataType()) {
            case STRING -> {
                String cursor = SCAN_POINTER_START;
                if (redisCursor != null) {
                    cursor = redisCursor.getCursor();
                }
                log.debug("Scanning new Redis keys from cursor %s . %d values read so far", cursor, totalValues);
                redisCursor = client.scan(cursor, scanParams);
                keys = new ArrayList<>(redisCursor.getResult());
            }
            case ZSET -> keys = new ArrayList<>(client.zrange(split.getKeyName(), split.getStart(), split.getEnd()));
            default -> log.warn("Redis key of type %s is unsupported", split.getKeyDataFormat());
        }
    }

    private void fetchData(List<String> currentKeys)
    {
        stringValues = null;
        hashValues = null;

        switch (split.getValueDataType()) {
            case STRING -> {
                if (clusterEnabled) {
                    stringValues = fetchStringValuesCluster(currentKeys);
                }
                else {
                    stringValues = client.mget(currentKeys.toArray(new String[0]));
                }
            }
            case HASH -> {
                if (clusterEnabled) {
                    hashValues = fetchHashValuesCluster(currentKeys);
                }
                else {
                    hashValues = fetchHashValuesStandalone(currentKeys);
                }
            }
            default -> log.warn("Redis value of type %s is unsupported", split.getValueDataType());
        }
    }

    /**
     * Fetches string values from a cluster primary with MOVED/ASK retry and
     * primary-failover handling.  When the split's primary becomes unreachable,
     * the topology is refreshed and keys are resolved to the new primary.
     */
    private static List<String> toArrayList(String[] results)
    {
        // List.of rejects null elements, but Redis GET may legitimately return null.
        return new ArrayList<>(Arrays.asList(results));
    }

    private List<String> fetchStringValuesCluster(List<String> currentKeys)
    {
        String[] results = new String[currentKeys.size()];
        List<Integer> pendingIndices = new ArrayList<>();
        List<String> pendingKeys = new ArrayList<>();
        for (int i = 0; i < currentKeys.size(); i++) {
            pendingIndices.add(i);
            pendingKeys.add(currentKeys.get(i));
        }

        for (int attempt = 0; attempt < MAX_REDIRECTION_RETRIES && !pendingKeys.isEmpty(); attempt++) {
            List<Object> replies;
            try {
                try (Pipeline pipeline = client.pipelined()) {
                    for (String key : pendingKeys) {
                        pipeline.get(key);
                    }
                    replies = pipeline.syncAndReturnAll();
                }
            }
            catch (JedisConnectionException e) {
                if (!clusterEnabled) {
                    throw e;
                }
                log.warn(e, "Redis cluster primary unreachable for split %s; refreshing topology", split.getNodes());
                List<Integer> nextPendingIndices = new ArrayList<>();
                List<String> nextPendingKeys = resolveKeysAfterConnectionFailure(pendingKeys, pendingIndices, results, RedisClient::get, nextPendingIndices);
                if (nextPendingKeys.isEmpty()) {
                    break;
                }
                pendingIndices = nextPendingIndices;
                pendingKeys = nextPendingKeys;
                continue;
            }

            List<Integer> nextPendingIndices = new ArrayList<>();
            List<String> nextPendingKeys = new ArrayList<>();

            for (int i = 0; i < replies.size(); i++) {
                int originalIndex = pendingIndices.get(i);
                String key = pendingKeys.get(i);
                Object reply = replies.get(i);

                if (reply instanceof JedisDataException jedisDataException) {
                    if (isRedirectionError(jedisDataException)) {
                        HostAddress target = RedisClientManager.parseRedirectionTarget(jedisDataException);
                        if (target == null) {
                            throw new TrinoException(
                                    GENERIC_INTERNAL_ERROR,
                                    "Malformed cluster redirection error for key " + key + ": " + jedisDataException.getMessage());
                        }
                        // Retry on the target node
                        try {
                            String value;
                            if (RedisClientManager.isAskRedirection(jedisDataException)) {
                                // ASK: slot is migrating, must send ASKING before command
                                value = clientManager.askAndGet(target, key);
                            }
                            else {
                                // MOVED: topology changed, retry with normal GET on target
                                RedisClient targetClient = clientManager.getClient(target);
                                value = targetClient.get(key);
                            }
                            results[originalIndex] = value;
                        }
                        catch (JedisDataException retryException) {
                            if (RedisClientManager.isAskRedirection(retryException)) {
                                // MOVED target returned ASK — slot is migrating, follow ASK
                                HostAddress askTarget = RedisClientManager.parseRedirectionTarget(retryException);
                                if (askTarget != null) {
                                    results[originalIndex] = clientManager.askAndGet(askTarget, key);
                                }
                                else {
                                    throw new TrinoException(
                                            GENERIC_INTERNAL_ERROR,
                                            "Malformed ASK redirection from MOVED target for key " + key + ": " + retryException.getMessage());
                                }
                            }
                            else if (isRedirectionError(retryException)) {
                                // Still redirected after retry — queue for next attempt
                                nextPendingIndices.add(originalIndex);
                                nextPendingKeys.add(key);
                            }
                            else {
                                throw retryException;
                            }
                        }
                        // On MOVED, refresh the cached topology
                        if (RedisClientManager.isMovedRedirection(jedisDataException)) {
                            log.info("MOVED redirect for key %s, refreshing cluster topology", key);
                            clientManager.refreshTopology();
                        }
                    }
                    else {
                        throw jedisDataException;
                    }
                }
                else {
                    results[originalIndex] = (String) reply;
                }
            }

            pendingIndices = nextPendingIndices;
            pendingKeys = nextPendingKeys;
        }

        if (!pendingKeys.isEmpty()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Exhausted " + MAX_REDIRECTION_RETRIES + " retries for cluster redirection(s) on keys: " + pendingKeys);
        }

        return toArrayList(results);
    }

    /**
     * Fetches hash values from a cluster primary with MOVED/ASK retry and
     * primary-failover handling.
     */
    private List<Object> fetchHashValuesCluster(List<String> currentKeys)
    {
        Object[] results = new Object[currentKeys.size()];
        List<Integer> pendingIndices = new ArrayList<>();
        List<String> pendingKeys = new ArrayList<>();
        for (int i = 0; i < currentKeys.size(); i++) {
            pendingIndices.add(i);
            pendingKeys.add(currentKeys.get(i));
        }

        for (int attempt = 0; attempt < MAX_REDIRECTION_RETRIES && !pendingKeys.isEmpty(); attempt++) {
            List<Object> replies;
            try {
                try (Pipeline pipeline = client.pipelined()) {
                    for (String key : pendingKeys) {
                        pipeline.hgetAll(key);
                    }
                    replies = pipeline.syncAndReturnAll();
                }
            }
            catch (JedisConnectionException e) {
                if (!clusterEnabled) {
                    throw e;
                }
                log.warn(e, "Redis cluster primary unreachable for hash split %s; refreshing topology", split.getNodes());
                List<Integer> nextPendingIndices = new ArrayList<>();
                List<String> nextPendingKeys = resolveKeysAfterConnectionFailure(pendingKeys, pendingIndices, results, RedisClient::hgetAll, nextPendingIndices);
                if (nextPendingKeys.isEmpty()) {
                    break;
                }
                pendingIndices = nextPendingIndices;
                pendingKeys = nextPendingKeys;
                continue;
            }

            List<Integer> nextPendingIndices = new ArrayList<>();
            List<String> nextPendingKeys = new ArrayList<>();

            for (int i = 0; i < replies.size(); i++) {
                int originalIndex = pendingIndices.get(i);
                String key = pendingKeys.get(i);
                Object reply = replies.get(i);

                if (reply instanceof JedisDataException jedisDataException) {
                    if (isRedirectionError(jedisDataException)) {
                        HostAddress target = RedisClientManager.parseRedirectionTarget(jedisDataException);
                        if (target == null) {
                            throw new TrinoException(
                                    GENERIC_INTERNAL_ERROR,
                                    "Malformed cluster redirection error for key " + key + ": " + jedisDataException.getMessage());
                        }
                        try {
                            Map<String, String> value;
                            if (RedisClientManager.isAskRedirection(jedisDataException)) {
                                // ASK: slot is migrating, must send ASKING before command
                                value = clientManager.askAndGetAll(target, key);
                            }
                            else {
                                // MOVED: topology changed, retry with normal HGETALL on target
                                RedisClient targetClient = clientManager.getClient(target);
                                value = targetClient.hgetAll(key);
                            }
                            results[originalIndex] = value;
                        }
                        catch (JedisDataException retryException) {
                            if (RedisClientManager.isAskRedirection(retryException)) {
                                // MOVED target returned ASK — slot is migrating, follow ASK
                                HostAddress askTarget = RedisClientManager.parseRedirectionTarget(retryException);
                                if (askTarget != null) {
                                    results[originalIndex] = clientManager.askAndGetAll(askTarget, key);
                                }
                                else {
                                    throw new TrinoException(
                                            GENERIC_INTERNAL_ERROR,
                                            "Malformed ASK redirection from MOVED target for hash key " + key + ": " + retryException.getMessage());
                                }
                            }
                            else if (isRedirectionError(retryException)) {
                                nextPendingIndices.add(originalIndex);
                                nextPendingKeys.add(key);
                            }
                            else {
                                throw retryException;
                            }
                        }
                        if (RedisClientManager.isMovedRedirection(jedisDataException)) {
                            log.info("MOVED redirect for hash key %s, refreshing cluster topology", key);
                            clientManager.refreshTopology();
                        }
                    }
                    else {
                        throw jedisDataException;
                    }
                }
                else {
                    results[originalIndex] = reply;
                }
            }

            pendingIndices = nextPendingIndices;
            pendingKeys = nextPendingKeys;
        }

        if (!pendingKeys.isEmpty()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Exhausted " + MAX_REDIRECTION_RETRIES + " retries for cluster redirection(s) on hash keys: " + pendingKeys);
        }

        return new ArrayList<>(Arrays.asList(results));
    }

    private List<String> resolveKeysAfterConnectionFailure(
            List<String> pendingKeys,
            List<Integer> pendingIndices,
            Object[] results,
            KeyFetcher fetcher,
            List<Integer> nextPendingIndices)
    {
        clientManager.refreshTopology();
        if (split.getClusterKeysOptional().isEmpty()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Redis cluster primary became unreachable during scan");
        }

        List<String> nextPendingKeys = new ArrayList<>();
        for (int i = 0; i < pendingKeys.size(); i++) {
            String key = pendingKeys.get(i);
            int originalIndex = pendingIndices.get(i);
            try {
                RedisClient primaryClient = clientManager.getClientForKey(key);
                Object value = fetcher.fetch(primaryClient, key);
                results[originalIndex] = value;
            }
            catch (JedisDataException dataException) {
                if (isRedirectionError(dataException)) {
                    nextPendingIndices.add(originalIndex);
                    nextPendingKeys.add(key);
                }
                else {
                    throw dataException;
                }
            }
            catch (JedisConnectionException e) {
                nextPendingIndices.add(originalIndex);
                nextPendingKeys.add(key);
            }
        }
        return nextPendingKeys;
    }

    @FunctionalInterface
    private interface KeyFetcher
    {
        Object fetch(RedisClient client, String key);
    }

    private List<Object> fetchHashValuesStandalone(List<String> currentKeys)
    {
        try (Pipeline pipeline = client.pipelined()) {
            for (String key : currentKeys) {
                pipeline.hgetAll(key);
            }
            return pipeline.syncAndReturnAll();
        }
    }
}
