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
import io.trino.server.protocol.QueryDataProducer;
import io.trino.server.protocol.QueryResultRows;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import jakarta.ws.rs.core.UriBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.Segment.inlined;
import static io.trino.client.spooling.Segment.spooled;
import static io.trino.server.protocol.spooling.CoordinatorSegmentResource.spooledSegmentUriBuilder;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;

public class SpoolingQueryDataProducer
        implements QueryDataProducer
{
    private boolean closed;
    private final QueryDataEncoder.Factory encoderFactory;
    private QueryDataEncoder encoder;

    private long currentOffset;

    public SpoolingQueryDataProducer(QueryDataEncoder.Factory encoderFactory)
    {
        this.encoderFactory = requireNonNull(encoderFactory, "encoderFactory is null");
    }

    @Override
    public QueryData produce(ExternalUriInfo uriInfo, Session session, QueryResultRows rows, Consumer<TrinoException> throwableConsumer)
    {
        if (rows.isEmpty()) {
            return null;
        }

        verify(!closed, "SpoolingQueryDataProducer is already closed");
        EncodedQueryData.Builder builder = EncodedQueryData.builder(encoderFactory.encoding());
        UriBuilder uriBuilder = spooledSegmentUriBuilder(uriInfo);
        if (encoder == null) {
            encoder = encoderFactory.create(session, rows.getOutputColumns());
            builder.withAttributes(encoder.attributes());
        }

        List<OutputColumn> outputColumns = rows.getOutputColumns();

        try {
            for (Page page : rows.getPages()) {
                if (hasSpoolingMetadata(page, outputColumns.size())) {
                    SpooledBlock metadata = SpooledBlock.deserialize(page);
                    DataAttributes attributes = metadata.attributes().toBuilder()
                            .set(ROW_OFFSET, currentOffset)
                            .build();
                    builder.withSegment(spooled(
                            metadata.directUri()
                                    .orElseGet(() -> buildSegmentDownloadURI(uriBuilder, metadata.identifier())),
                            buildSegmentAckURI(uriBuilder, metadata.identifier()),
                            attributes,
                            metadata.headers()));
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

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }

        // For empty results encoder will never be created
        if (encoder != null) {
            encoder.close();
        }
        encoder = null;
        closed = true;
    }

    private URI buildSegmentDownloadURI(UriBuilder builder, Slice identifier)
    {
        return builder.clone().path("download/{identifier}").build(identifier.toStringUtf8());
    }

    private URI buildSegmentAckURI(UriBuilder builder, Slice identifier)
    {
        return builder.clone().path("ack/{identifier}").build(identifier.toStringUtf8());
    }

    private boolean hasSpoolingMetadata(Page page, int outputColumnsSize)
    {
        return page.getChannelCount() == outputColumnsSize + 1 && page.getPositionCount() == 1 && !page.getBlock(outputColumnsSize).isNull(0);
    }
}
