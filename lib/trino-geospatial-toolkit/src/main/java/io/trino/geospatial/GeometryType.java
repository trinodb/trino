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
package io.trino.geospatial;

import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public enum GeometryType
{
    POINT(false, utf8Slice("ST_Point")),
    MULTI_POINT(true, utf8Slice("ST_MultiPoint")),
    LINE_STRING(false, utf8Slice("ST_LineString")),
    MULTI_LINE_STRING(true, utf8Slice("ST_MultiLineString")),
    POLYGON(false, utf8Slice("ST_Polygon")),
    MULTI_POLYGON(true, utf8Slice("ST_MultiPolygon")),
    GEOMETRY_COLLECTION(true, utf8Slice("ST_GeomCollection"));

    private final boolean multitype;
    private final Slice standardName;

    GeometryType(boolean multitype, Slice standardName)
    {
        this.multitype = multitype;
        this.standardName = standardName;
    }

    public boolean isMultitype()
    {
        return multitype;
    }

    public Slice standardName()
    {
        return standardName;
    }

    public static GeometryType getForEsriGeometryType(String type)
    {
        return getForInternalLibraryName(type);
    }

    public static GeometryType getForJtsGeometryType(String type)
    {
        return getForInternalLibraryName(type);
    }

    private static GeometryType getForInternalLibraryName(String type)
    {
        requireNonNull(type, "type is null");
        return switch (type) {
            case "Point" -> POINT;
            case "MultiPoint" -> MULTI_POINT;
            case "LineString" -> LINE_STRING;
            case "MultiLineString" -> MULTI_LINE_STRING;
            case "Polygon" -> POLYGON;
            case "MultiPolygon" -> MULTI_POLYGON;
            case "GeometryCollection" -> GEOMETRY_COLLECTION;
            default -> throw new IllegalArgumentException("Invalid Geometry Type: " + type);
        };
    }
}
