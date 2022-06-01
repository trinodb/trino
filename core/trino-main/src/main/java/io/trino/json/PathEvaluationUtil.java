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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PathEvaluationUtil
{
    private PathEvaluationUtil() {}

    public static List<Object> unwrapArrays(List<Object> sequence)
    {
        return sequence.stream()
                .flatMap(object -> {
                    if (object instanceof JsonNode && ((JsonNode) object).getNodeType() == ARRAY) {
                        return ImmutableList.copyOf(((JsonNode) object).elements()).stream();
                    }
                    return Stream.of(object);
                })
                .collect(toImmutableList());
    }
}
