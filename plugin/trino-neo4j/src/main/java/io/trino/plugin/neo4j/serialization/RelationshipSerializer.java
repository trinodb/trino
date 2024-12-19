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
package io.trino.plugin.neo4j.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.neo4j.driver.types.Relationship;

import java.io.IOException;

public class RelationshipSerializer
        extends JsonSerializer<Relationship>
{
    @Override
    public void serialize(Relationship value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        gen.writeStartObject();
        gen.writeObjectField("elementId", value.elementId());
        gen.writeObjectField("type", value.type());
        gen.writeObjectField("startNodeElementId", value.startNodeElementId());
        gen.writeObjectField("endNodeElementId", value.endNodeElementId());
        gen.writeObjectField("properties", value.asMap());
        gen.writeEndObject();
    }
}
