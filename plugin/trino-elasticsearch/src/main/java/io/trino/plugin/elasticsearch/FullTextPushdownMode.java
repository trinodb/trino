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
 * Controls how predicates that may require Elasticsearch full-text semantics are pushed down.
 *
 * <p>SAFE is a correctness mode, not merely a residual-filter mode. A remote candidate is allowed only when it is
 * proven not to introduce false negatives for the SQL predicate. Keeping a Trino residual can remove remote false
 * positives, but it cannot recover rows that Elasticsearch filtered out. Any translation that can lose SQL matches is
 * therefore available only in UNSAFE mode and is marked APPROXIMATE in the Remote Predicate IR.</p>
 */
public enum FullTextPushdownMode
{
    /**
     * Strict, default. Predicates requiring non-exact full-text semantics are left to Trino.
     */
    DISABLED,
    /**
     * Push only proven no-false-negative candidates and keep the exact SQL predicate as a Trino residual. Analyzed-text
     * translations are not used merely because a residual exists; they must first satisfy the candidate-safety proof.
     */
    SAFE,
    /**
     * Push a valid Elasticsearch translation and trust the remote result even when analyzer/token semantics can differ
     * from SQL. Such predicates are marked APPROXIMATE and do not retain an exact residual solely for equivalence.
     */
    UNSAFE,
}
