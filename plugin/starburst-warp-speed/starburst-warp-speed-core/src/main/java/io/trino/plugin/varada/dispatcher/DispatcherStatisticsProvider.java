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
package io.trino.plugin.varada.dispatcher;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.statistics.Estimate;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherStatisticsProvider
{
    private final Map<Integer, Integer> cardinalityMap;

    @Inject
    public DispatcherStatisticsProvider(GlobalConfiguration globalConfiguration)
    {
        String cardinalityBucketConfig = requireNonNull(globalConfiguration).getCardinalityBuckets();
        cardinalityMap = parseCardinalityConfig(cardinalityBucketConfig);
    }

    public int getColumnCardinalityBucket(Estimate estimate)
    {
        for (Map.Entry<Integer, Integer> bucket : cardinalityMap.entrySet()) {
            if (estimate.getValue() > bucket.getKey()) {
                return bucket.getValue();
            }
        }
        return 0;
    }

    private Map<Integer, Integer> parseCardinalityConfig(String cardinalityBucketConfig)
    {
        NavigableMap<Integer, Integer> cardinalityMap = new TreeMap<>();
        String[] buckets = cardinalityBucketConfig.split(",", -1);
        int bucketPriority = 1;
        int previousBucket = 0;
        for (String bucket : buckets) {
            int currentBucket = Integer.parseInt(bucket);
            if (currentBucket <= previousBucket) {
                throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_BUCKET_CONFIGURATION, "invalid bucket configuration");
            }
            cardinalityMap.put(currentBucket, bucketPriority);
            bucketPriority++;
            previousBucket = currentBucket;
        }
        return ImmutableMap.copyOf(cardinalityMap.descendingMap());
    }
}
