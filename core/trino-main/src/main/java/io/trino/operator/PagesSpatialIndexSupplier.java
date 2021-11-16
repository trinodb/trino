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
package io.trino.operator;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.geospatial.Rectangle;
import io.trino.geospatial.rtree.Flatbush;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.PagesRTreeIndex.GeometryWithPosition;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.geospatial.serde.GeometrySerde.deserialize;
import static io.trino.operator.PagesSpatialIndex.EMPTY_INDEX;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesSpatialIndexSupplier
        implements Supplier<PagesSpatialIndex>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesSpatialIndexSupplier.class).instanceSize();
    private static final int MEMORY_USAGE_UPDATE_INCREMENT_BYTES = 100 * 1024 * 1024;   // 100 MB

    private final Session session;
    private final LongArrayList addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final Optional<Integer> radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final Flatbush<GeometryWithPosition> rtree;
    private final Map<Integer, Rectangle> partitions;
    private final long memorySizeInBytes;

    public PagesSpatialIndexSupplier(
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions,
            LocalMemoryContext localUserMemoryContext)
    {
        requireNonNull(localUserMemoryContext, "localUserMemoryContext is null");
        this.session = session;
        this.addresses = addresses;
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = channels;
        this.spatialRelationshipTest = spatialRelationshipTest;
        this.filterFunctionFactory = filterFunctionFactory;
        this.partitions = partitions;

        this.rtree = buildRTree(addresses, channels, geometryChannel, radiusChannel, partitionChannel, localUserMemoryContext);
        this.radiusChannel = radiusChannel;
        this.memorySizeInBytes = INSTANCE_SIZE + rtree.getEstimatedSizeInBytes();
    }

    private static Flatbush<GeometryWithPosition> buildRTree(LongArrayList addresses, List<List<Block>> channels, int geometryChannel, Optional<Integer> radiusChannel, Optional<Integer> partitionChannel, LocalMemoryContext localUserMemoryContext)
    {
        Operator relateOperator = OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate);

        ObjectArrayList<GeometryWithPosition> geometries = new ObjectArrayList<>();

        long recordedSizeInBytes = localUserMemoryContext.getBytes();
        long addedSizeInBytes = 0;

        for (int position = 0; position < addresses.size(); position++) {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            Block block = channels.get(geometryChannel).get(blockIndex);
            // TODO Consider pushing is-null and is-empty checks into a filter below the join
            if (block.isNull(blockPosition)) {
                continue;
            }

            Slice slice = block.getSlice(blockPosition, 0, block.getSliceLength(blockPosition));
            OGCGeometry ogcGeometry = deserialize(slice);
            verifyNotNull(ogcGeometry);
            if (ogcGeometry.isEmpty()) {
                continue;
            }

            double radius = radiusChannel.map(channel -> DOUBLE.getDouble(channels.get(channel).get(blockIndex), blockPosition)).orElse(0.0);
            if (radius < 0) {
                continue;
            }

            if (radiusChannel.isEmpty()) {
                // If radiusChannel is supplied, this is a distance query, for which our acceleration won't help.
                accelerateGeometry(ogcGeometry, relateOperator);
            }

            int partition = -1;
            if (partitionChannel.isPresent()) {
                Block partitionBlock = channels.get(partitionChannel.get()).get(blockIndex);
                partition = toIntExact(INTEGER.getLong(partitionBlock, blockPosition));
            }

            GeometryWithPosition geometryWithPosition = new GeometryWithPosition(ogcGeometry, partition, position, radius);
            geometries.add(geometryWithPosition);

            addedSizeInBytes += geometryWithPosition.getEstimatedSizeInBytes();

            if (addedSizeInBytes >= MEMORY_USAGE_UPDATE_INCREMENT_BYTES) {
                localUserMemoryContext.setBytes(recordedSizeInBytes + addedSizeInBytes);
                recordedSizeInBytes += addedSizeInBytes;
                addedSizeInBytes = 0;
            }
        }
        return new Flatbush<>(geometries.toArray(new GeometryWithPosition[] {}));
    }

    private static void accelerateGeometry(OGCGeometry ogcGeometry, Operator relateOperator)
    {
        // Recurse into GeometryCollections
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        while (true) {
            com.esri.core.geometry.Geometry esriGeometry = cursor.next();
            if (esriGeometry == null) {
                break;
            }
            relateOperator.accelerateGeometry(esriGeometry, null, Geometry.GeometryAccelerationDegree.enumMild);
        }
    }

    // doesn't include memory used by channels and addresses which are shared with PagesIndex
    public DataSize getEstimatedSize()
    {
        return DataSize.ofBytes(memorySizeInBytes);
    }

    @Override
    public PagesSpatialIndex get()
    {
        if (rtree.isEmpty()) {
            return EMPTY_INDEX;
        }
        return new PagesRTreeIndex(session, addresses, types, outputChannels, channels, rtree, radiusChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }
}
