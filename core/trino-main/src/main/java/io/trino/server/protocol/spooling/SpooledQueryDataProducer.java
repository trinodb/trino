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
import io.trino.Session;
import io.trino.client.QueryData;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.QueryResultRows;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.protocol.SpooledLocation.CoordinatorLocation;
import io.trino.spi.protocol.SpooledLocation.DirectLocation;
import jakarta.ws.rs.core.UriBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.Segment.inlined;
import static io.trino.client.spooling.Segment.spooled;
import static io.trino.server.protocol.spooling.SegmentResource.spooledSegmentUriBuilder;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;

public class SpooledQueryDataProducer
        implements QueryDataProducer
{
    private final QueryDataEncoder.Factory encoderFactory;
    private final AtomicBoolean metadataWritten = new AtomicBoolean(false);

    private long currentOffset;

    public SpooledQueryDataProducer(QueryDataEncoder.Factory encoderFactory)
    {
        this.encoderFactory = requireNonNull(encoderFactory, "encoderFactory is null");
    }

    @Override
    public QueryData produce(ExternalUriInfo uriInfo, Session session, QueryResultRows rows, Consumer<TrinoException> throwableConsumer)
    {
        if (rows.isEmpty()) {
            return null;
        }

        UriBuilder uriBuilder = spooledSegmentUriBuilder(uriInfo);
        QueryDataEncoder encoder = encoderFactory.create(session, rows.getOutputColumns().orElseThrow());
        EncodedQueryData.Builder builder = EncodedQueryData.builder(encoder.encoding());
        List<OutputColumn> outputColumns = rows.getOutputColumns().orElseThrow();

        if (metadataWritten.compareAndSet(false, true)) {
            // Attributes are emitted only once for the first segment
            builder.withAttributes(encoder.attributes());
        }

        try {
            for (Page page : rows.getPages()) {
                if (hasSpoolingMetadata(page, outputColumns)) {
                    SpooledBlock metadata = SpooledBlock.deserialize(page);
                    DataAttributes attributes = metadata.attributes().toBuilder()
                            .set(ROW_OFFSET, currentOffset)
                            .build();

                    builder.withSegment(switch (metadata.location()) {
                        case CoordinatorLocation coordinatorLocation ->
                                spooled(buildSegmentURI(uriBuilder, coordinatorLocation.identifier()), attributes, coordinatorLocation.headers());
                        case DirectLocation directLocation ->
                                spooled(directLocation.uri(), attributes, directLocation.headers());
                    });
                    currentOffset += attributes.get(ROWS_COUNT, Long.class);
                }
                else {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    DataAttributes attributes = encoder.encodeTo(output, List.of(page))
                            .toBuilder()
                            .set(ROW_OFFSET, currentOffset)
                            .build();
                    builder.withSegment(inlined(output.toByteArray(), attributes));
                    currentOffset += page.getPositionCount();
                }
            }
        }
        catch (IOException e) {
            throwableConsumer.accept(new TrinoException(SERIALIZATION_ERROR, "Failed to serialize query data", e));
        }
        catch (TrinoException e) {
            throwableConsumer.accept(e);
            return null;
        }

        return builder.build();
    }

    private URI buildSegmentURI(UriBuilder builder, Slice identifier)
    {
        return builder.clone().build(identifier.toStringUtf8());
    }

    private boolean hasSpoolingMetadata(Page page, List<OutputColumn> outputColumns)
    {
        return page.getChannelCount() == outputColumns.size() + 1 && page.getPositionCount() == 1 && !page.getBlock(outputColumns.size()).isNull(0);
    }

    public static QueryDataProducer createSpooledQueryDataProducer(QueryDataEncoder.Factory encoder)
    {
        return new SpooledQueryDataProducer(encoder);
    }
}
