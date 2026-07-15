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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Writes VARBINARY values annotated with a Geometry logical type, additionally
 * feeding each WKB payload through a parquet-column
 * {@link org.apache.parquet.column.statistics.geospatial.GeospatialStatistics.Builder}
 * so that the resulting bounding box can be emitted on the column chunk's
 * {@code geospatial_statistics} thrift field.
 */
public class GeometryValueWriter
        extends BinaryValueWriter
{
    private final GeospatialStatistics.Builder geospatialStatisticsBuilder;

    public GeometryValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(valuesWriter, parquetType);
        this.geospatialStatisticsBuilder = GeospatialStatistics.newBuilder(parquetType);
    }

    public GeospatialStatistics.Builder getGeospatialStatisticsBuilder()
    {
        return geospatialStatisticsBuilder;
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        super.writeValueBlock(block);
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                updateGeospatialStatistics(variableWidthBlock, i);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        super.writeRepeated(block, count);
        // Repeating a value does not change the bounding box
        updateGeospatialStatistics((VariableWidthBlock) block, 0);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        super.writePositions(block, positions, offset, length);
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        boolean mayHaveNull = block.mayHaveNull();
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                updateGeospatialStatistics(variableWidthBlock, position);
            }
        }
    }

    private void updateGeospatialStatistics(VariableWidthBlock block, int position)
    {
        Slice rawSlice = block.getRawSlice();
        geospatialStatisticsBuilder.update(Binary.fromReusedByteArray(
                rawSlice.byteArray(),
                rawSlice.byteArrayOffset() + block.getRawSliceOffset(position),
                block.getSliceLength(position)));
    }
}
