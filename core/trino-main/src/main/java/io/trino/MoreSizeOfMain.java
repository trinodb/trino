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
package io.trino;

import com.google.common.collect.Multimap;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.ToLongFunction;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;

public final class MoreSizeOfMain
{
    private static final int SIMPLE_ENTRY_INSTANCE_SIZE = instanceSize(AbstractMap.SimpleEntry.class);

    private static final long DATE_TIME_INSTANCE_SIZE = instanceSize(DateTime.class);
    private static final long DURATION_INSTANCE_SIZE = instanceSize(Duration.class);
    private static final long DATA_SIZE_INSTANCE_SIZE = instanceSize(DataSize.class);
    private static final long DISTRIBUTION_SNAPSHOT_INSTANCE_SIZE = instanceSize(Distribution.DistributionSnapshot.class);

    private MoreSizeOfMain() {}

    public static long sizeOf(DateTime dateTime)
    {
        return DATE_TIME_INSTANCE_SIZE;
    }

    public static long sizeOf(Duration duration)
    {
        return DURATION_INSTANCE_SIZE;
    }

    public static long sizeOf(DataSize dataSize)
    {
        return DATA_SIZE_INSTANCE_SIZE;
    }

    public static long sizeOf(Distribution.DistributionSnapshot distributionSnapshot)
    {
        return DISTRIBUTION_SNAPSHOT_INSTANCE_SIZE;
    }

    public static <K, V> long estimatedSizeOf(Multimap<K, V> map, ToLongFunction<K> keySize, ToLongFunction<V> valueSize)
    {
        if (map == null) {
            return 0;
        }

        long result = sizeOfObjectArray(map.size());
        for (Map.Entry<K, V> entry : map.entries()) {
            result += SIMPLE_ENTRY_INSTANCE_SIZE +
                    keySize.applyAsLong(entry.getKey()) +
                    valueSize.applyAsLong(entry.getValue());
        }
        return result;
    }
}
