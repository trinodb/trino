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
import org.neo4j.driver.types.Point;

import java.io.IOException;

public class PointSerializer
        extends JsonSerializer<Point>
{
    @Override
    public void serialize(Point value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        gen.writeStartObject();
        switch (value.srid()) {
            // https://neo4j.com/docs/cypher-manual/current/functions/spatial/#functions-point-wgs84-2d
            case 4326 -> {
                gen.writeObjectField("longitude", value.x());
                gen.writeObjectField("latitude", value.y());
            }
            // https://neo4j.com/docs/cypher-manual/current/functions/spatial/#functions-point-wgs84-3d
            case 4979 -> {
                gen.writeObjectField("longitude", value.x());
                gen.writeObjectField("latitude", value.y());
                gen.writeObjectField("height", value.z());
            }
            // https://neo4j.com/docs/cypher-manual/current/functions/spatial/#functions-point-cartesian-2d
            case 7203 -> {
                gen.writeObjectField("x", value.x());
                gen.writeObjectField("y", value.y());
            }
            // https://neo4j.com/docs/cypher-manual/current/functions/spatial/#functions-point-cartesian-3d
            case 9157 -> {
                gen.writeObjectField("x", value.x());
                gen.writeObjectField("y", value.y());
                gen.writeObjectField("z", value.z());
            }
        }
        gen.writeObjectField("srid", value.srid());
        gen.writeEndObject();
    }
}
