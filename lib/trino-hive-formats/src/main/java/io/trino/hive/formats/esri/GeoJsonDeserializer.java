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
package io.trino.hive.formats.esri;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.JsonParserReader;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonParser;
import io.trino.hive.formats.line.Column;
import io.trino.spi.PageBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;

public final class GeoJsonDeserializer
        extends AbstractDeserializer
{
    private static final SpatialReference SPATIAL_REFERENCE = SpatialReference.create(4326);
    private static final String GEOMETRY_FIELD_NAME = "geometry";
    private static final String PROPERTIES_FIELD_NAME = "properties";

    public GeoJsonDeserializer(List<Column> columns)
    {
        super(columns);
    }

    public void deserialize(PageBuilder pageBuilder, JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != START_OBJECT) {
            throw invalidJson("start of object expected");
        }

        Arrays.fill(fieldWritten, false);
        while (nextObjectField(parser)) {
            String fieldName = parser.currentName();
            if (nextTokenRequired(parser) == VALUE_NULL) {
                continue;
            }
            if (GEOMETRY_FIELD_NAME.equals(fieldName)) {
                parseGeometry(parser).ifPresent(ogcGeometry -> writeGeometry(pageBuilder, ogcGeometry));
            }
            else if (PROPERTIES_FIELD_NAME.equals(fieldName)) {
                parseAttributes(parser, pageBuilder, PROPERTIES_FIELD_NAME); // TODO support rowtype?
            }
            else {
                skipCurrentValue(parser);
            }
        }

        pageBuilder.declarePosition();

        for (int i = 0; i < columns.size(); i++) {
            if (!fieldWritten[i]) {
                pageBuilder.getBlockBuilder(i).appendNull();
            }
        }
    }

    private Optional<OGCGeometry> parseGeometry(JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != START_OBJECT) {
            throw invalidJson("geometry is not an object");
        }

        // if geometry is not mapped to a column, skip it
        if (geometryColumn <= -1) {
            skipCurrentValue(parser);
            return Optional.empty();
        }

        MapGeometry mapGeometry = OperatorImportFromGeoJson.local().execute(0, Geometry.Type.Unknown, new JsonParserReader(parser), null);
        return Optional.of(OGCGeometry.createFromEsriGeometry(mapGeometry.getGeometry(), SPATIAL_REFERENCE));
    }
}
