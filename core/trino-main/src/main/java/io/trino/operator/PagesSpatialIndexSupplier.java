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
import io.trino.operator.PagesRTreeIndex.GeometryWithPosition;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verifyNotNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.geospatial.serde.GeometrySerde.deserialize;
import static io.trino.operator.PagesSpatialIndex.EMPTY_INDEX;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;

public class PagesSpatialIndexSupplier
        implements Supplier<PagesSpatialIndex>
{
    private static final int INSTANCE_SIZE = instanceSize(PagesSpatialIndexSupplier.class);
    private static final int ENVELOPE_INSTANCE_SIZE = instanceSize(Envelope.class);
    private static final int STRTREE_INSTANCE_SIZE = instanceSize(STRtree.class);
    private static final int ABSTRACT_NODE_INSTANCE_SIZE = instanceSize(AbstractNode.class);

    private final Session session;
    private final LongArrayList addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<ObjectArrayList<Block>> channels;
    private final Optional<Integer> radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final STRtree rtree;
    private final Map<Integer, Rectangle> partitions;
    private final long memorySizeInBytes;

    public PagesSpatialIndexSupplier(
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            List<ObjectArrayList<Block>> channels,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions)
    {
        this.session = session;
        this.addresses = addresses;
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = channels;
        this.spatialRelationshipTest = spatialRelationshipTest;
        this.filterFunctionFactory = filterFunctionFactory;
        this.partitions = partitions;

        this.rtree = buildRTree(addresses, channels, geometryChannel, radiusChannel, partitionChannel);
        this.radiusChannel = radiusChannel;
        this.memorySizeInBytes = INSTANCE_SIZE +
                (rtree.isEmpty() ? 0 : STRTREE_INSTANCE_SIZE + computeMemorySizeInBytes(rtree.getRoot()));
    }

    private static STRtree buildRTree(LongArrayList addresses, List<ObjectArrayList<Block>> channels, int geometryChannel, Optional<Integer> radiusChannel, Optional<Integer> partitionChannel)
    {
        STRtree rtree = new STRtree();
        Operator relateOperator = OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate);

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
                partition = INTEGER.getInt(partitionBlock, blockPosition);
            }

            rtree.insert(getEnvelope(ogcGeometry, radius), new GeometryWithPosition(ogcGeometry, partition, position));
        }

        rtree.build();
        return rtree;
    }

    private static Envelope getEnvelope(OGCGeometry ogcGeometry, double radius)
    {
        com.esri.core.geometry.Envelope envelope = new com.esri.core.geometry.Envelope();
        ogcGeometry.getEsriGeometry().queryEnvelope(envelope);

        return new Envelope(envelope.getXMin() - radius, envelope.getXMax() + radius, envelope.getYMin() - radius, envelope.getYMax() + radius);
    }

    private long computeMemorySizeInBytes(AbstractNode root)
    {
        if (root.getLevel() == 0) {
            return ABSTRACT_NODE_INSTANCE_SIZE + ENVELOPE_INSTANCE_SIZE + root.getChildBoundables().stream().mapToLong(child -> computeMemorySizeInBytes((ItemBoundable) child)).sum();
        }
        return ABSTRACT_NODE_INSTANCE_SIZE + ENVELOPE_INSTANCE_SIZE + root.getChildBoundables().stream().mapToLong(child -> computeMemorySizeInBytes((AbstractNode) child)).sum();
    }

    private long computeMemorySizeInBytes(ItemBoundable item)
    {
        return ENVELOPE_INSTANCE_SIZE + ((GeometryWithPosition) item.getItem()).getEstimatedMemorySizeInBytes();
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
