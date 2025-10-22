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

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.node.JsonNodeType;
import tools.jackson.databind.node.ValueNode;

public class JsonEmptySequenceNode
        extends ValueNode
{
    public static final JsonEmptySequenceNode EMPTY_SEQUENCE = new JsonEmptySequenceNode();

    private JsonEmptySequenceNode() {}

    @Override
    public JsonToken asToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNodeType getNodeType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "EMPTY_SEQUENCE";
    }

    @Override
    protected String _valueDesc()
    {
        return "EMPTY_SEQUENCE";
    }

    @Override
    public boolean equals(Object o)
    {
        return o == this;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public void serialize(JsonGenerator gen, SerializationContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializationContext context, TypeSerializer typeSer)
    {
        throw new UnsupportedOperationException();
    }
}
