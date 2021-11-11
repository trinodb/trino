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
package io.trino.plugin.elasticsearch.utility;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public enum ElasticsearchDataTypes
{
    // Whole numbers
    BYTE(0, 0), SHORT(0, 1), INTEGER(0, 2), LONG(0, 3),

    // Decimal numbers
    HALF_FLOAT(1, 0), FLOAT(1, 1), DOUBLE(1, 2);

    int bucket;
    int weight;

    public int getBucket()
    {
        return bucket;
    }

    public int getWeight()
    {
        return weight;
    }

    ElasticsearchDataTypes(int bucket, int weight)
    {
        this.bucket = bucket;
        this.weight = weight;
    }

    public static String getWiderDataType(Collection<String> dataTypes)
    {
        try {
            List<ElasticsearchDataTypes> elasticsearchDataTypes = dataTypes.stream().map(String::toUpperCase).map(ElasticsearchDataTypes::valueOf).collect(Collectors.toList());

            final long numberOfBuckets = elasticsearchDataTypes.stream().map(ElasticsearchDataTypes::getBucket).distinct().count();
            if (numberOfBuckets > 1) {
                return "text";
            }
            return _widerType(elasticsearchDataTypes);
        }
        catch (IllegalArgumentException e) {
            // Do nothing.
        }

        return "text";
    }

    private static String _widerType(List<ElasticsearchDataTypes> types)
    {
        PriorityQueue<ElasticsearchDataTypes> heap = new PriorityQueue<>(Comparator.comparingInt(ElasticsearchDataTypes::getWeight).reversed());
        heap.addAll(types);
        return heap.poll().name();
    }
}
