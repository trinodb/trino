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

import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.parquet.format.BoundingBox;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.GeospatialStatistics;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TestParquetUtilGeospatialBounds
{
    @Test
    void testGeometryBoundsAreCollected()
            throws Exception
    {
        FileMetaData fileMetaData = buildFileMetaData(
                "geom",
                LogicalType.GEOMETRY(new GeometryType().setCrs("OGC:CRS84")),
                bboxWithXy(-10.0, 10.0, -20.0, 20.0),
                42);

        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId("test"), Optional.empty());

        // Sanity: raw thrift geospatial stats are exposed via the accessor.
        assertThat(metadata.getGeospatialStatisticsByColumn())
                .as("expected getGeospatialStatisticsByColumn to include geom")
                .isNotEmpty();

        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault());

        ByteBuffer lower = metrics.lowerBounds().get(42);
        ByteBuffer upper = metrics.upperBounds().get(42);
        assertThat(lower).as("lower bound for geom").isNotNull();
        assertThat(upper).as("upper bound for geom").isNotNull();

        GeospatialBound lowerBound = GeospatialBound.fromByteBuffer(lower.order(ByteOrder.LITTLE_ENDIAN));
        GeospatialBound upperBound = GeospatialBound.fromByteBuffer(upper.order(ByteOrder.LITTLE_ENDIAN));
        assertThat(lowerBound.x()).isEqualTo(-10.0);
        assertThat(lowerBound.y()).isEqualTo(-20.0);
        assertThat(lowerBound.hasZ()).isFalse();
        assertThat(lowerBound.hasM()).isFalse();
        assertThat(upperBound.x()).isEqualTo(10.0);
        assertThat(upperBound.y()).isEqualTo(20.0);
    }

    @Test
    void testGeographyBoundsAreCollected()
            throws Exception
    {
        FileMetaData fileMetaData = buildFileMetaData(
                "geog",
                LogicalType.GEOGRAPHY(new GeographyType().setCrs("OGC:CRS84")),
                bboxWithXy(1.5, 2.5, 3.5, 4.5),
                7);

        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId("test"), Optional.empty());

        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault());

        ByteBuffer lower = metrics.lowerBounds().get(7);
        ByteBuffer upper = metrics.upperBounds().get(7);
        assertThat(lower).isNotNull();
        assertThat(upper).isNotNull();

        GeospatialBound lowerBound = GeospatialBound.fromByteBuffer(lower.order(ByteOrder.LITTLE_ENDIAN));
        GeospatialBound upperBound = GeospatialBound.fromByteBuffer(upper.order(ByteOrder.LITTLE_ENDIAN));
        assertThat(lowerBound.x()).isEqualTo(1.5);
        assertThat(lowerBound.y()).isEqualTo(3.5);
        assertThat(upperBound.x()).isEqualTo(2.5);
        assertThat(upperBound.y()).isEqualTo(4.5);
    }

    @Test
    void testGeometryBoundsMergedAcrossRowGroups()
            throws Exception
    {
        FileMetaData fileMetaData = buildMultiRowGroupFileMetaData(
                "geom",
                LogicalType.GEOMETRY(new GeometryType().setCrs("OGC:CRS84")),
                List.of(
                        bboxWithXy(-5.0, 5.0, -6.0, 6.0),
                        bboxWithXy(-20.0, -1.0, -30.0, 4.0)),
                42);

        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId("test"), Optional.empty());

        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault());

        GeospatialBound lower = GeospatialBound.fromByteBuffer(metrics.lowerBounds().get(42).order(ByteOrder.LITTLE_ENDIAN));
        GeospatialBound upper = GeospatialBound.fromByteBuffer(metrics.upperBounds().get(42).order(ByteOrder.LITTLE_ENDIAN));
        assertThat(lower.x()).isEqualTo(-20.0);
        assertThat(lower.y()).isEqualTo(-30.0);
        assertThat(upper.x()).isEqualTo(5.0);
        assertThat(upper.y()).isEqualTo(6.0);
    }

    @Test
    void testGeometryBoundsDroppedWhenAnyRowGroupMissingStats()
            throws Exception
    {
        // Second row group carries no geospatial stats; the column should be dropped from bounds.
        FileMetaData fileMetaData = buildMultiRowGroupFileMetaData(
                "geom",
                LogicalType.GEOMETRY(new GeometryType().setCrs("OGC:CRS84")),
                Arrays.asList(bboxWithXy(-5.0, 5.0, -6.0, 6.0), null),
                42);

        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId("test"), Optional.empty());

        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault());

        assertThat(metrics.lowerBounds().get(42)).isNull();
        assertThat(metrics.upperBounds().get(42)).isNull();
    }

    @Test
    void testGeometryBoundsSkippedWhenBoundingBoxAbsent()
            throws Exception
    {
        FileMetaData fileMetaData = buildFileMetaData(
                "geom",
                LogicalType.GEOMETRY(new GeometryType().setCrs("OGC:CRS84")),
                null,
                42);

        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId("test"), Optional.empty());

        Metrics metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.getDefault());

        assertThat(metrics.lowerBounds().get(42)).isNull();
        assertThat(metrics.upperBounds().get(42)).isNull();
    }

    private static BoundingBox bboxWithXy(double xmin, double xmax, double ymin, double ymax)
    {
        return new BoundingBox()
                .setXmin(xmin)
                .setXmax(xmax)
                .setYmin(ymin)
                .setYmax(ymax);
    }

    private static FileMetaData buildMultiRowGroupFileMetaData(String columnName, LogicalType logicalType, List<BoundingBox> bboxPerRowGroup, int fieldId)
    {
        SchemaElement root = new SchemaElement().setName("root").setNum_children(1);
        SchemaElement column = new SchemaElement()
                .setName(columnName)
                .setType(Type.BYTE_ARRAY)
                .setRepetition_type(FieldRepetitionType.OPTIONAL)
                .setLogicalType(logicalType)
                .setField_id(fieldId);

        List<RowGroup> rowGroups = new ArrayList<>(bboxPerRowGroup.size());
        for (BoundingBox bbox : bboxPerRowGroup) {
            ColumnMetaData columnMetaData = new ColumnMetaData(
                    Type.BYTE_ARRAY,
                    List.of(Encoding.PLAIN),
                    List.of(columnName),
                    CompressionCodec.UNCOMPRESSED,
                    10L,
                    200L,
                    100L,
                    4L);
            columnMetaData.setTotal_compressed_size(100L);
            if (bbox != null) {
                columnMetaData.setGeospatial_statistics(new GeospatialStatistics().setBbox(bbox));
            }
            ColumnChunk columnChunk = new ColumnChunk(4L);
            columnChunk.setMeta_data(columnMetaData);
            rowGroups.add(new RowGroup(List.of(columnChunk), 100L, 10L));
        }

        return new FileMetaData(1, List.of(root, column), 10L * bboxPerRowGroup.size(), rowGroups);
    }

    private static FileMetaData buildFileMetaData(String columnName, LogicalType logicalType, BoundingBox bbox, int fieldId)
    {
        // Root schema element
        SchemaElement root = new SchemaElement().setName("root").setNum_children(1);
        SchemaElement column = new SchemaElement()
                .setName(columnName)
                .setType(Type.BYTE_ARRAY)
                .setRepetition_type(FieldRepetitionType.OPTIONAL)
                .setLogicalType(logicalType)
                .setField_id(fieldId);

        ColumnMetaData columnMetaData = new ColumnMetaData(
                Type.BYTE_ARRAY,
                List.of(Encoding.PLAIN),
                List.of(columnName),
                CompressionCodec.UNCOMPRESSED,
                10L,
                200L,
                100L,
                4L);
        columnMetaData.setTotal_compressed_size(100L);
        if (bbox != null) {
            columnMetaData.setGeospatial_statistics(new GeospatialStatistics().setBbox(bbox));
        }

        ColumnChunk columnChunk = new ColumnChunk(4L);
        columnChunk.setMeta_data(columnMetaData);

        RowGroup rowGroup = new RowGroup(List.of(columnChunk), 100L, 10L);

        return new FileMetaData(1, List.of(root, column), 10L, List.of(rowGroup));
    }
}
