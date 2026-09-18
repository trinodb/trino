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
package io.trino.plugin.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import static org.apache.parquet.schema.LogicalTypeAnnotation.DEFAULT_ALGO;
import static org.apache.parquet.schema.LogicalTypeAnnotation.geographyType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergParquetSchemaConverter
{
    @Test
    void testGeospatialLogicalAnnotations()
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "geom_default", Types.GeometryType.crs84()),
                Types.NestedField.optional(2, "geom_3857", Types.GeometryType.of("EPSG:3857")),
                Types.NestedField.optional(3, "geog_default", Types.GeographyType.crs84()),
                Types.NestedField.optional(4, "row_payload", Types.StructType.of(
                        Types.NestedField.optional(5, "geom", Types.GeometryType.crs84()))),
                Types.NestedField.optional(6, "array_payload", Types.ListType.ofOptional(7, Types.GeometryType.of("EPSG:3857"))),
                Types.NestedField.optional(8, "map_payload", Types.MapType.ofOptional(9, 10, Types.StringType.get(), Types.GeographyType.crs84())));

        MessageType messageType = IcebergParquetSchemaConverter.convert(schema, "table");

        assertGeometry(messageType.getType("geom_default").asPrimitiveType(), geometryType("OGC:CRS84"));
        assertGeometry(messageType.getType("geom_3857").asPrimitiveType(), geometryType("EPSG:3857"));
        assertGeography(messageType.getType("geog_default").asPrimitiveType(), geographyType("OGC:CRS84", DEFAULT_ALGO));
        assertGeometry(messageType.getType("row_payload").asGroupType().getType("geom").asPrimitiveType(), geometryType("OGC:CRS84"));
        assertGeometry(messageType.getType("array_payload").asGroupType().getType("list").asGroupType().getType("element").asPrimitiveType(), geometryType("EPSG:3857"));
        assertGeography(messageType.getType("map_payload").asGroupType().getType("key_value").asGroupType().getType("value").asPrimitiveType(), geographyType("OGC:CRS84", DEFAULT_ALGO));
    }

    private static void assertGeometry(PrimitiveType primitiveType, LogicalTypeAnnotation expectedAnnotation)
    {
        assertThat(primitiveType.getPrimitiveTypeName()).isEqualTo(BINARY);
        assertThat(primitiveType.getLogicalTypeAnnotation()).isEqualTo(expectedAnnotation);
    }

    private static void assertGeography(PrimitiveType primitiveType, LogicalTypeAnnotation expectedAnnotation)
    {
        assertThat(primitiveType.getPrimitiveTypeName()).isEqualTo(BINARY);
        assertThat(primitiveType.getLogicalTypeAnnotation()).isEqualTo(expectedAnnotation);
    }
}
