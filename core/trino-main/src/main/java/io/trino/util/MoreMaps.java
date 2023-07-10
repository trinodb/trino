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
package io.trino.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.fail;

public final class MoreMaps
{
    private MoreMaps() {}

    public static <K, V> Map<K, V> mergeMaps(Map<K, V> map1, Map<K, V> map2, BinaryOperator<V> merger)
    {
        return mergeMaps(Stream.of(map1, map2), merger);
    }

    public static <K, V> Map<K, V> mergeMaps(Stream<Map<K, V>> mapStream, BinaryOperator<V> merger)
    {
        return mapStream
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, merger));
    }

    // this help function should only be used when the map contains null value
    // otherwise, use ImmutableMap.of()
    public static <K, V> Map<K, V> asMap(List<K> keyList, List<V> valueList)
    {
        if (keyList.size() != valueList.size()) {
            fail("keyList should have same size with valueList");
        }
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            if (map.put(keyList.get(i), valueList.get(i)) != null) {
                fail("keyList should have same size with valueList");
            }
        }
        return map;
    }
}
