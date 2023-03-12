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
package io.trino.spi.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.EXCLUDED;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.INCLUDED;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.UNKNOWN;
import static java.lang.Math.max;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class ExchangeSourceOutputSelector
{
    private static final long INSTANCE_SIZE = instanceSize(ExchangeSourceOutputSelector.class);

    private final int version;
    private final Map<ExchangeId, Slice> values;
    private final boolean finalSelector;

    // visible for Jackson
    @JsonCreator
    public ExchangeSourceOutputSelector(
            @JsonProperty("version") int version,
            @JsonProperty("values") Map<ExchangeId, Slice> values,
            @JsonProperty("finalSelector") boolean finalSelector)
    {
        this.version = version;
        this.values = Map.copyOf(requireNonNull(values, "values is null"));
        this.finalSelector = finalSelector;
    }

    @JsonProperty
    public int getVersion()
    {
        return version;
    }

    // visible for Jackson
    @JsonProperty
    public Map<ExchangeId, Slice> getValues()
    {
        return values;
    }

    @JsonProperty("finalSelector")
    public boolean isFinal()
    {
        return finalSelector;
    }

    public Selection getSelection(ExchangeId exchangeId, int taskPartitionId, int attemptId)
    {
        requireNonNull(exchangeId, "exchangeId is null");
        if (taskPartitionId < 0) {
            throw new IllegalArgumentException("unexpected taskPartitionId: " + taskPartitionId);
        }
        if (attemptId < 0 || attemptId > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("unexpected attemptId: " + attemptId);
        }
        Slice exchangeValues = values.get(exchangeId);
        if (exchangeValues == null) {
            throwIfFinal(exchangeId, taskPartitionId);
            return UNKNOWN;
        }
        if (exchangeValues.length() <= taskPartitionId) {
            throwIfFinal(exchangeId, taskPartitionId);
            return UNKNOWN;
        }
        byte selectedAttempt = exchangeValues.getByte(taskPartitionId);
        if (selectedAttempt == UNKNOWN.getValue()) {
            throwIfFinal(exchangeId, taskPartitionId);
            return UNKNOWN;
        }
        if (selectedAttempt == EXCLUDED.getValue()) {
            return EXCLUDED;
        }
        if (selectedAttempt < 0) {
            throw new IllegalArgumentException("unexpected selectedAttempt: " + selectedAttempt);
        }
        return selectedAttempt == attemptId ? INCLUDED : EXCLUDED;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SizeOf.estimatedSizeOf(values, ExchangeId::getRetainedSizeInBytes, Slice::getRetainedSize);
    }

    public void checkValidTransition(ExchangeSourceOutputSelector newSelector)
    {
        if (this.version >= newSelector.version) {
            throw new IllegalArgumentException("Invalid transition to the same or an older version");
        }

        if (this.isFinal()) {
            throw new IllegalArgumentException("Invalid transition from final selector");
        }

        Set<ExchangeId> exchangeIds = new HashSet<>();
        exchangeIds.addAll(this.values.keySet());
        exchangeIds.addAll(newSelector.values.keySet());

        for (ExchangeId exchangeId : exchangeIds) {
            int taskPartitionCount = max(this.getPartitionCount(exchangeId), newSelector.getPartitionCount(exchangeId));
            for (int taskPartitionId = 0; taskPartitionId < taskPartitionCount; taskPartitionId++) {
                byte currentValue = this.getValue(exchangeId, taskPartitionId);
                byte newValue = newSelector.getValue(exchangeId, taskPartitionId);
                if (currentValue == UNKNOWN.getValue()) {
                    // transition from UNKNOWN is always valid
                    continue;
                }
                if (currentValue != newValue) {
                    throw new IllegalArgumentException("Invalid transition for exchange %s, taskPartitionId %s: %s -> %s".formatted(exchangeId, taskPartitionId, currentValue, newValue));
                }
            }
        }
    }

    public ExchangeSourceOutputSelector merge(ExchangeSourceOutputSelector other)
    {
        Map<ExchangeId, Slice> values = new HashMap<>(this.values);
        other.values.forEach((exchangeId, value) -> {
            Slice currentValue = values.putIfAbsent(exchangeId, value);
            if (currentValue != null) {
                throw new IllegalArgumentException("duplicated selector for exchange: " + exchangeId);
            }
        });
        return new ExchangeSourceOutputSelector(
                this.version + other.version,
                values,
                this.finalSelector && other.finalSelector);
    }

    private int getPartitionCount(ExchangeId exchangeId)
    {
        Slice values = this.values.get(exchangeId);
        if (values == null) {
            return 0;
        }
        return values.length();
    }

    private byte getValue(ExchangeId exchangeId, int taskPartitionId)
    {
        Slice exchangeValues = values.get(exchangeId);
        if (exchangeValues == null) {
            return UNKNOWN.getValue();
        }
        if (exchangeValues.length() <= taskPartitionId) {
            return UNKNOWN.getValue();
        }
        return exchangeValues.getByte(taskPartitionId);
    }

    private void throwIfFinal(ExchangeId exchangeId, int taskPartitionId)
    {
        if (isFinal()) {
            throw new IllegalArgumentException("selection not found for exchangeId %s, taskPartitionId %s".formatted(exchangeId, taskPartitionId));
        }
    }

    public enum Selection
    {
        INCLUDED((byte) -1),
        EXCLUDED((byte) -2),
        UNKNOWN((byte) -3);

        private final byte value;

        Selection(byte value)
        {
            this.value = value;
        }

        public byte getValue()
        {
            return value;
        }
    }

    public static Builder builder(Set<ExchangeId> sourceExchanges)
    {
        return new Builder(sourceExchanges);
    }

    public static class Builder
    {
        private int nextVersion;
        private final Map<ExchangeId, ValuesBuilder> exchangeValues;
        private boolean finalSelector;
        private final Map<ExchangeId, Integer> exchangeTaskPartitionCount = new HashMap<>();

        public Builder(Set<ExchangeId> sourceExchanges)
        {
            requireNonNull(sourceExchanges, "sourceExchanges is null");
            exchangeValues = sourceExchanges.stream()
                    .collect(toUnmodifiableMap(Function.identity(), exchangeId -> new ValuesBuilder()));
        }

        public Builder include(ExchangeId exchangeId, int taskPartitionId, int attemptId)
        {
            getValuesBuilderForExchange(exchangeId).include(taskPartitionId, attemptId);
            return this;
        }

        public Builder exclude(ExchangeId exchangeId, int taskPartitionId)
        {
            getValuesBuilderForExchange(exchangeId).exclude(taskPartitionId);
            return this;
        }

        private ValuesBuilder getValuesBuilderForExchange(ExchangeId exchangeId)
        {
            ValuesBuilder result = exchangeValues.get(exchangeId);
            if (result == null) {
                throw new IllegalArgumentException("Unexpected exchange: " + exchangeId);
            }
            return result;
        }

        public Builder setPartitionCount(ExchangeId exchangeId, int count)
        {
            Integer previousCount = exchangeTaskPartitionCount.putIfAbsent(exchangeId, count);
            if (previousCount != null) {
                throw new IllegalStateException("Partition count for exchange is already set: " + exchangeId);
            }
            return this;
        }

        public Builder setFinal()
        {
            if (finalSelector) {
                throw new IllegalStateException("selector is already marked as final");
            }
            for (ExchangeId exchangeId : exchangeValues.keySet()) {
                if (!exchangeTaskPartitionCount.containsKey(exchangeId)) {
                    throw new IllegalStateException("partition count is missing for exchange: " + exchangeId);
                }
            }
            this.finalSelector = true;
            return this;
        }

        public ExchangeSourceOutputSelector build()
        {
            return new ExchangeSourceOutputSelector(
                    nextVersion++,
                    exchangeValues.entrySet().stream()
                            .collect(toMap(Map.Entry::getKey, entry -> {
                                ExchangeId exchangeId = entry.getKey();
                                ValuesBuilder valuesBuilder = entry.getValue();
                                if (finalSelector) {
                                    return valuesBuilder.buildFinal(exchangeTaskPartitionCount.get(exchangeId));
                                }
                                else {
                                    return valuesBuilder.build();
                                }
                            })),
                    finalSelector);
        }
    }

    private static class ValuesBuilder
    {
        private Slice values = Slices.allocate(0);
        private int maxTaskPartitionId = -1;

        public void include(int taskPartitionId, int attemptId)
        {
            updateMaxTaskPartitionIdAndEnsureCapacity(taskPartitionId);
            if (attemptId < 0 || attemptId > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("unexpected attemptId: " + attemptId);
            }
            byte currentValue = values.getByte(taskPartitionId);
            if (currentValue != UNKNOWN.getValue()) {
                throw new IllegalArgumentException("decision for partition %s is already made: %s".formatted(taskPartitionId, currentValue));
            }
            values.setByte(taskPartitionId, (byte) attemptId);
        }

        public void exclude(int taskPartitionId)
        {
            updateMaxTaskPartitionIdAndEnsureCapacity(taskPartitionId);
            byte currentValue = values.getByte(taskPartitionId);
            if (currentValue != UNKNOWN.getValue()) {
                throw new IllegalArgumentException("decision for partition %s is already made: %s".formatted(taskPartitionId, currentValue));
            }
            values.setByte(taskPartitionId, EXCLUDED.getValue());
        }

        private void updateMaxTaskPartitionIdAndEnsureCapacity(int taskPartitionId)
        {
            if (taskPartitionId > maxTaskPartitionId) {
                maxTaskPartitionId = taskPartitionId;
            }
            if (taskPartitionId < values.length()) {
                return;
            }
            byte[] newValues = new byte[(maxTaskPartitionId + 1) * 2];
            fill(newValues, UNKNOWN.getValue());
            values.getBytes(0, newValues, 0, values.length());
            values = Slices.wrappedBuffer(newValues);
        }

        public Slice build()
        {
            return createResult(maxTaskPartitionId + 1);
        }

        public Slice buildFinal(int totalPartitionCount)
        {
            Slice result = createResult(totalPartitionCount);
            for (int partitionId = 0; partitionId < totalPartitionCount; partitionId++) {
                byte selectedAttempt = result.getByte(partitionId);
                if (selectedAttempt == UNKNOWN.getValue()) {
                    throw new IllegalStateException("Attempt is unknown for partition: " + partitionId);
                }
            }
            return result;
        }

        private Slice createResult(int partitionCount)
        {
            if (maxTaskPartitionId >= partitionCount) {
                throw new IllegalArgumentException("expected maxTaskPartitionId to be less than or equal to " + (partitionCount - 1));
            }
            byte[] result = new byte[partitionCount];
            fill(result, UNKNOWN.getValue());
            values.getBytes(0, result, 0, maxTaskPartitionId + 1);
            return Slices.wrappedBuffer(result);
        }
    }
}
