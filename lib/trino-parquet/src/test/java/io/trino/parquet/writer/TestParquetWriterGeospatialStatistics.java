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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.parquet.format.BoundingBox;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.GeospatialStatistics;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;

class TestParquetWriterGeospatialStatistics
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final String COLUMN_NAME = "geom";

    @Test
    void writeGeometryColumnEmitsGeospatialStatistics()
            throws Exception
    {
        MessageType messageType = geometryMessageType(LogicalTypeAnnotation.geometryType("OGC:CRS84"));

        Slice writtenBytes = writeGeometryFile(messageType, ImmutableList.of(
                point2d(-10.0, -20.0),
                point2d(10.0, 20.0),
                point2d(3.0, 4.0)));

        GeospatialStatistics stats = readOnlyGeospatialStatistics(writtenBytes);
        assertThat(stats).isNotNull();
        assertThat(stats.isSetBbox()).isTrue();

        BoundingBox bbox = stats.getBbox();
        assertThat(bbox.getXmin()).isEqualTo(-10.0);
        assertThat(bbox.getXmax()).isEqualTo(10.0);
        assertThat(bbox.getYmin()).isEqualTo(-20.0);
        assertThat(bbox.getYmax()).isEqualTo(20.0);
        assertThat(bbox.isSetZmin()).isFalse();
        assertThat(bbox.isSetMmin()).isFalse();
    }

    @Test
    void writeGeometryColumnCapturesZExtent()
            throws Exception
    {
        MessageType messageType = geometryMessageType(LogicalTypeAnnotation.geometryType("OGC:CRS84"));

        Slice writtenBytes = writeGeometryFile(messageType, ImmutableList.of(
                point3d(-1.0, -2.0, 5.0),
                point3d(2.0, 3.0, 15.0)));

        GeospatialStatistics stats = readOnlyGeospatialStatistics(writtenBytes);
        assertThat(stats).isNotNull();

        BoundingBox bbox = stats.getBbox();
        assertThat(bbox.getXmin()).isEqualTo(-1.0);
        assertThat(bbox.getXmax()).isEqualTo(2.0);
        assertThat(bbox.getYmin()).isEqualTo(-2.0);
        assertThat(bbox.getYmax()).isEqualTo(3.0);
        assertThat(bbox.isSetZmin()).isTrue();
        assertThat(bbox.getZmin()).isEqualTo(5.0);
        assertThat(bbox.getZmax()).isEqualTo(15.0);
    }

    @Test
    void writeGeometryColumnSkipsStatsWhenAllValuesNull()
            throws Exception
    {
        MessageType messageType = geometryMessageType(LogicalTypeAnnotation.geometryType("OGC:CRS84"));

        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 3);
        builder.appendNull();
        builder.appendNull();
        builder.appendNull();

        Slice writtenBytes = writeParquetFile(messageType, ImmutableList.of(new Page(builder.build())));

        GeospatialStatistics stats = readOnlyGeospatialStatistics(writtenBytes);
        // An all-null column produces no valid bounding box; the field should be omitted entirely.
        assertThat(stats).isNull();
    }

    @Test
    void writeVarbinaryColumnDoesNotEmitGeospatialStatistics()
            throws Exception
    {
        MessageType messageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).named(COLUMN_NAME))
                .named("trino_test");

        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 2);
        VARBINARY.writeSlice(builder, Slices.wrappedBuffer(new byte[] {0x1, 0x2, 0x3}));
        VARBINARY.writeSlice(builder, Slices.wrappedBuffer(new byte[] {0x4, 0x5, 0x6}));

        Slice writtenBytes = writeParquetFile(messageType, ImmutableList.of(new Page(builder.build())));

        assertThat(readOnlyGeospatialStatistics(writtenBytes)).isNull();
    }

    @Test
    void writeGeographyColumnRemainsUnsetForNow()
            throws Exception
    {
        // parquet-column 1.17.1 returns a NoopBuilder for Geography annotations, so geospatial
        // statistics should not be emitted. This test documents that behavior — it will need
        // adjustment when upstream lands geography support.
        MessageType messageType = geometryMessageType(LogicalTypeAnnotation.geographyType());

        Slice writtenBytes = writeGeometryFile(messageType, ImmutableList.of(point2d(1.0, 2.0)));

        assertThat(readOnlyGeospatialStatistics(writtenBytes)).isNull();
    }

    private static Slice writeGeometryFile(MessageType messageType, List<byte[]> wkbValues)
            throws Exception
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, wkbValues.size());
        for (byte[] wkb : wkbValues) {
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(wkb));
        }
        return writeParquetFile(messageType, ImmutableList.of(new Page(builder.build())));
    }

    private static Slice writeParquetFile(MessageType messageType, List<Page> pages)
            throws Exception
    {
        Map<List<String>, Type> primitiveTypes = Map.of(ImmutableList.of(COLUMN_NAME), (Type) VarbinaryType.VARBINARY);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ParquetWriter writer = new ParquetWriter(
                outputStream,
                messageType,
                primitiveTypes,
                ParquetWriterOptions.builder().build(),
                CompressionCodec.UNCOMPRESSED,
                "test-version",
                Optional.of(DateTimeZone.UTC),
                Optional.empty())) {
            for (Page page : pages) {
                writer.write(page);
            }
        }
        return Slices.wrappedBuffer(outputStream.toByteArray());
    }

    private static GeospatialStatistics readOnlyGeospatialStatistics(Slice writtenBytes)
            throws Exception
    {
        TestingParquetDataSource dataSource = new TestingParquetDataSource(writtenBytes, ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        List<RowGroup> rowGroups = parquetMetadata.getParquetMetadata().getRow_groups();
        assertThat(rowGroups).hasSize(1);
        List<ColumnChunk> columns = rowGroups.get(0).getColumns();
        assertThat(columns).hasSize(1);
        return columns.get(0).getMeta_data().isSetGeospatial_statistics()
                ? columns.get(0).getMeta_data().getGeospatial_statistics()
                : null;
    }

    private static MessageType geometryMessageType(LogicalTypeAnnotation annotation)
    {
        PrimitiveType column = Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
                .as(annotation)
                .named(COLUMN_NAME);
        return Types.buildMessage().addField(column).named("trino_test");
    }

    private static byte[] point2d(double x, double y)
    {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
        return new WKBWriter().write(point);
    }

    private static byte[] point3d(double x, double y, double z)
    {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y, z));
        return new WKBWriter(3).write(point);
    }
}
