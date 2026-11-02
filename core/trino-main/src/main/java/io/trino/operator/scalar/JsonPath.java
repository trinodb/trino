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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.json.Json;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class JsonPath
{
    private final String pattern;
    private final JsonExtract.JsonExtractor<Slice> scalarExtractor;
    private final JsonExtract.JsonExtractor<Slice> objectExtractor;
    private final JsonExtract.JsonExtractor<Long> sizeExtractor;
    private final List<PathElement> elements;

    public JsonPath(String pattern)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        scalarExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.ScalarValueJsonExtractor());
        objectExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonValueJsonExtractor());
        sizeExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonSizeExtractor());
        ImmutableList.Builder<PathElement> elements = ImmutableList.builder();
        new JsonPathTokenizer(pattern).forEachRemaining(token -> {
            int index = -1;
            try {
                index = Integer.parseInt(token);
            }
            catch (NumberFormatException _) {
            }
            elements.add(new PathElement(utf8Slice(token), index));
        });
        this.elements = elements.build();
    }

    Json extract(Json value)
    {
        for (PathElement element : elements) {
            if (value.isObject()) {
                value = value.objectMember(element.key()).orElse(null);
            }
            else if (value.isArray() && element.index() >= 0 && element.index() < value.arraySize()) {
                value = value.arrayElement(element.index());
            }
            else {
                return null;
            }
            if (value == null) {
                return null;
            }
        }
        return value;
    }

    private record PathElement(Slice key, int index) {}

    public String pattern()
    {
        return pattern;
    }

    public JsonExtract.JsonExtractor<Slice> getScalarExtractor()
    {
        return scalarExtractor;
    }

    public JsonExtract.JsonExtractor<Slice> getObjectExtractor()
    {
        return objectExtractor;
    }

    public JsonExtract.JsonExtractor<Long> getSizeExtractor()
    {
        return sizeExtractor;
    }

    @Override
    public String toString()
    {
        return pattern;
    }
}
