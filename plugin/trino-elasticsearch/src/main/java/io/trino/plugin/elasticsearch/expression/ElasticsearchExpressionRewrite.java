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
package io.trino.plugin.elasticsearch.expression;

import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;

import static java.util.Objects.requireNonNull;

public record ElasticsearchExpressionRewrite(
        ElasticsearchColumnHandle column,
        QueryType queryType,
        String value)
{
    public ElasticsearchExpressionRewrite
    {
        requireNonNull(column, "column is null");
        requireNonNull(queryType, "queryType is null");
        requireNonNull(value, "value is null");
    }

    public enum QueryType
    {
        MATCH_PHRASE
    }
}
