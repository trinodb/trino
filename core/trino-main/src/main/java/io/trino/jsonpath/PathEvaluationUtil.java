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
package io.trino.jsonpath;

import com.google.common.collect.ImmutableList;
import io.trino.json.Json;

import java.util.List;

public final class PathEvaluationUtil
{
    private PathEvaluationUtil() {}

    /// In lax mode, every JSON array in the input sequence is automatically unwrapped
    /// into its elements before applying methods, accessors, and arithmetic. Non-array
    /// items pass through unchanged.
    public static List<Json> unwrapArrays(List<Json> sequence)
    {
        ImmutableList.Builder<Json> builder = ImmutableList.builder();
        for (Json item : sequence) {
            if (item.isArray()) {
                item.forEachArrayElement(builder::add);
            }
            else {
                builder.add(item);
            }
        }
        return builder.build();
    }
}
