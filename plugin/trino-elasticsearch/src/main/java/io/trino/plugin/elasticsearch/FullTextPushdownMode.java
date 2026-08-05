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
 * Controls how predicates and dynamic filters on analyzed {@code text} fields are pushed to Elasticsearch as
 * full-text queries. Because a {@code text} field is tokenized, a full-text match does not have exact SQL
 * equality semantics, so this trades correctness for pushdown and is disabled by default.
 */
public enum FullTextPushdownMode
{
    /**
     * Strict, default. Predicates on analyzed {@code text} fields are left to the engine (exact SQL semantics).
     */
    DISABLED,
    /**
     * Push a full-text query as a pre-filter but keep the exact predicate as a residual filter, so the engine
     * re-applies it. Eliminates false positives; a value that analyzes to nothing (for example a stop word) can
     * still be dropped, so results are exact only for typical values.
     */
    SAFE,
    /**
     * Push a full-text query and trust the Elasticsearch result (no residual). Fully pushed down and fastest, but
     * results follow Elasticsearch analysis semantics (tokenization, lowercasing, stemming) and can differ from SQL.
     */
    UNSAFE,
}
