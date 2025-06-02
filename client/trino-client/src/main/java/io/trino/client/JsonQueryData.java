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
package io.trino.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;

import static java.util.Objects.requireNonNull;

public class JsonQueryData
        implements QueryData
{
    private final JsonNode node;

    public JsonQueryData(JsonNode node)
    {
        this.node = requireNonNull(node, "node is null");
        if (node.isNull()) {
            throw new IllegalArgumentException("JsonNode cannot be null");
        }
    }

    public JsonNode getNode()
    {
        return node;
    }

    public JsonParser getJsonParser()
    {
        return node.traverse();
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public long getRowsCount()
    {
        return node.size();
    }
}
