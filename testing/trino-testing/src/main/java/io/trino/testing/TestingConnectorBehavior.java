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
package io.trino.testing;

public enum TestingConnectorBehavior
{
    SUPPORTS_PREDICATE_PUSHDOWN,
    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
    SUPPORTS_LIMIT_PUSHDOWN,
    SUPPORTS_TOPN_PUSHDOWN,
    SUPPORTS_AGGREGATION_PUSHDOWN,
    SUPPORTS_JOIN_PUSHDOWN,
    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
    /**/;
}
