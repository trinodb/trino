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

/**
 * Resource-safety policy for predicate composition optimizations.
 *
 * <p>The policy is part of the permanent composer API. Defaults are deliberately conservative and can later be
 * supplied from connector/session configuration without changing the translation or composition contracts.</p>
 */
record ElasticsearchPredicateCompositionPolicy(int maxTermsValues, int termsBatchSize, int maxBooleanClauses, int maxQueryBytes)
{
    static final int DEFAULT_MAX_QUERY_BYTES = 1_048_576;
    static final ElasticsearchPredicateCompositionPolicy DEFAULT = new ElasticsearchPredicateCompositionPolicy(50_000, 1_000, 1_000, DEFAULT_MAX_QUERY_BYTES);

    ElasticsearchPredicateCompositionPolicy(int maxTermsValues, int termsBatchSize, int maxBooleanClauses)
    {
        this(maxTermsValues, termsBatchSize, maxBooleanClauses, DEFAULT_MAX_QUERY_BYTES);
    }

    ElasticsearchPredicateCompositionPolicy
    {
        if (maxTermsValues < 1 || termsBatchSize < 1 || maxBooleanClauses < 1 || maxQueryBytes < 1) {
            throw new IllegalArgumentException("Predicate composition limits must be positive");
        }
    }
}
