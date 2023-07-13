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
package io.trino.operator.aggregation.state;

import io.trino.array.ObjectBigArray;
import io.trino.operator.aggregation.KeyValuePairs;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class KeyValuePairsStateFactory
        implements AccumulatorStateFactory<KeyValuePairsState>
{
    private final Type keyType;
    private final Type valueType;

    public KeyValuePairsStateFactory(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public KeyValuePairsState createSingleState()
    {
        return new SingleState(keyType, valueType);
    }

    @Override
    public KeyValuePairsState createGroupedState()
    {
        return new GroupedState(keyType, valueType);
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements KeyValuePairsState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedState.class);
        private final Type keyType;
        private final Type valueType;
        private final ObjectBigArray<KeyValuePairs> pairs = new ObjectBigArray<>();
        private long size;

        public GroupedState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            pairs.ensureCapacity(size);
        }

        @Override
        public KeyValuePairs get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(KeyValuePairs value)
        {
            requireNonNull(value, "value is null");

            KeyValuePairs previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            pairs.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + pairs.sizeOf();
        }
    }

    public static class SingleState
            implements KeyValuePairsState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleState.class);
        private final Type keyType;
        private final Type valueType;
        private KeyValuePairs pair;

        public SingleState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        // for copying
        private SingleState(Type keyType, Type valueType, KeyValuePairs pair)
        {
            this.keyType = keyType;
            this.valueType = valueType;
            this.pair = pair;
        }

        @Override
        public KeyValuePairs get()
        {
            return pair;
        }

        @Override
        public void set(KeyValuePairs value)
        {
            pair = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (pair != null) {
                estimatedSize += pair.estimatedInMemorySize();
            }
            return estimatedSize;
        }

        @Override
        public AccumulatorState copy()
        {
            KeyValuePairs pairCopy = null;
            if (pair != null) {
                pairCopy = pair.copy();
            }
            return new SingleState(keyType, valueType, pairCopy);
        }
    }
}
