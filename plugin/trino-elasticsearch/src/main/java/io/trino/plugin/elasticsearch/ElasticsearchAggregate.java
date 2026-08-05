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
package io.trino.plugin.elasticsearch;

import io.trino.spi.type.Type;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A single metric aggregate pushed into Elasticsearch. {@code outputName} is the synthetic column name that carries
 * the result back to the engine; {@code field} is the Elasticsearch field the metric is computed over (absent for
 * {@code count(*)}).
 */
public record ElasticsearchAggregate(String outputName, Function function, Optional<String> field, Type outputType)
{
    public enum Function
    {
        // count(*) reads the bucket doc_count; the rest map to an Elasticsearch metric sub-aggregation
        COUNT_ALL, COUNT, SUM, MIN, MAX, AVG, COUNT_DISTINCT
    }

    public ElasticsearchAggregate
    {
        requireNonNull(outputName, "outputName is null");
        requireNonNull(function, "function is null");
        requireNonNull(field, "field is null");
        requireNonNull(outputType, "outputType is null");
    }
}
