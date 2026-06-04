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
package io.trino.plugin.iceberg;

import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

final class GeoSpatialUtils
{
    private GeoSpatialUtils() {}

    public static boolean isGeometryType(Type type)
    {
        return type.getBaseName().equals(StandardTypes.GEOMETRY);
    }

    public static boolean isSphericalGeographyType(Type type)
    {
        return type.getBaseName().equals(StandardTypes.SPHERICAL_GEOGRAPHY);
    }

    public static boolean isGeospatialType(Type type)
    {
        return isGeometryType(type) || isSphericalGeographyType(type);
    }

    public static Type getGeometryType(TypeManager typeManager)
    {
        return typeManager.getType(new TypeSignature(StandardTypes.GEOMETRY));
    }

    public static Type getSphericalGeographyType(TypeManager typeManager)
    {
        return typeManager.getType(new TypeSignature(StandardTypes.SPHERICAL_GEOGRAPHY));
    }
}
