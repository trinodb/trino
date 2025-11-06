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

import com.google.common.collect.ImmutableList;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static tools.jackson.databind.node.JsonNodeType.ARRAY;

public final class PathEvaluationUtil
{
    private PathEvaluationUtil() {}

    public static List<Object> unwrapArrays(List<Object> sequence)
    {
        return sequence.stream()
                .flatMap(object -> {
                    if (object instanceof JsonNode node && node.getNodeType() == ARRAY) {
                        return ImmutableList.copyOf(node.iterator()).stream();
                    }
                    return Stream.of(object);
                })
                .collect(toImmutableList());
    }
}
