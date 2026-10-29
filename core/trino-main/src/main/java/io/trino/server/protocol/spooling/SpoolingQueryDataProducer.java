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
package io.trino.server.protocol.spooling;

import io.airlift.slice.Slice;
import io.trino.client.QueryData;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.QueryDataProducer;
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpoolingManager;
import jakarta.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.function.Consumer;

import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.Segment.inlined;
import static io.trino.client.spooling.Segment.spooled;
import static io.trino.server.protocol.spooling.CoordinatorSegmentResource.spooledSegmentUriBuilder;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.STORAGE;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;

public class SpoolingQueryDataProducer
        implements QueryDataProducer
{
    private final String encoding;
    private final SpoolingManager spoolingManager;
    private final SegmentRetrievalMode retrievalMode;

    private long currentOffset;

    public SpoolingQueryDataProducer(String encoding, SpoolingManager spoolingManager, SegmentRetrievalMode retrievalMode)
    {
        this.encoding = requireNonNull(encoding, "encoding is null");
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.retrievalMode = requireNonNull(retrievalMode, "retrievalMode is null");
    }

    @Override
    public QueryData produce(ExternalUriInfo uriInfo, QueryResultRows rows, Consumer<TrinoException> throwableConsumer)
    {
        if (rows.isEmpty()) {
            return null;
        }
        EncodedQueryData.Builder builder = EncodedQueryData.builder(encoding);
        UriBuilder uriBuilder = spooledSegmentUriBuilder(uriInfo);
        try {
            for (Page page : rows.getPages()) {
                for (SpooledMetadataBlock metadataBlock : SpooledMetadataBlockSerde.deserialize(page)) {
                    DataAttributes attributes = metadataBlock.attributes().toBuilder()
                            .set(ROW_OFFSET, currentOffset)
                            .build();
                    switch (metadataBlock) {
                        case SpooledMetadataBlock.Spooled spooled -> builder.withSegment(createSpooledSegment(uriBuilder, spooled, attributes));
                        case SpooledMetadataBlock.Inlined inlined -> builder.withSegment(inlined(inlined.data().getBytes(), attributes));
                    }
                    currentOffset += attributes.get(ROWS_COUNT, Long.class);
                }
            }
        }
        catch (IOException | RuntimeException e) {
            throwableConsumer.accept(new TrinoException(SERIALIZATION_ERROR, "Failed to serialize spooled segments", e));
            return null;
        }
        return builder.build();
    }

    private Segment createSpooledSegment(UriBuilder uriBuilder, SpooledMetadataBlock.Spooled metadata, DataAttributes attributes)
            throws IOException
    {
        if (retrievalMode == STORAGE) {
            // Sign when the coordinator delivers the segment to the client, rather than when a worker spools it.
            DirectLocation location = spoolingManager.directLocation(spoolingManager.handle(metadata.identifier(), metadata.headers()))
                    .orElseThrow(() -> new IOException("Could not generate pre-signed URI"));
            return spooled(location.directUri(), buildSegmentAckURI(uriBuilder, metadata.identifier()), attributes, location.headers());
        }
        return spooled(
                metadata.directUri().orElseGet(() -> buildSegmentDownloadURI(uriBuilder, metadata.identifier())),
                buildSegmentAckURI(uriBuilder, metadata.identifier()),
                attributes,
                metadata.headers());
    }

    private URI buildSegmentDownloadURI(UriBuilder builder, Slice identifier)
    {
        return builder.clone().path("download/{identifier}").build(identifier.toStringUtf8());
    }

    private URI buildSegmentAckURI(UriBuilder builder, Slice identifier)
    {
        return builder.clone().path("ack/{identifier}").build(identifier.toStringUtf8());
    }
}
