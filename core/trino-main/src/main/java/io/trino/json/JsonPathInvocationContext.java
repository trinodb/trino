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
package io.trino.json;

/**
 * A class representing state, used by JSON-processing functions: JSON_EXISTS, JSON_VALUE, and JSON_QUERY.
 * It is instantiated per driver, and allows gathering and reusing information across the processed rows.
 * It contains a JsonPathEvaluator object, which caches ResolvedFunctions used by certain path nodes.
 * Caching the ResolvedFunctions addresses the assumption that all or most rows shall provide values
 * of the same types to certain JSON path operators.
 */
public class JsonPathInvocationContext
{
    private JsonPathEvaluator evaluator;

    public JsonPathEvaluator getEvaluator()
    {
        return evaluator;
    }

    public void setEvaluator(JsonPathEvaluator evaluator)
    {
        this.evaluator = evaluator;
    }
}
