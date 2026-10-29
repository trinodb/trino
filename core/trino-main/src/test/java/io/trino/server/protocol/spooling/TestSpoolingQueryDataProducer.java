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

import com.google.inject.Key;
import io.airlift.jaxrs.testing.MockUriInfo;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.SpooledSegment;
import io.trino.node.InternalNode;
import io.trino.server.ExternalUriInfo;
import io.trino.server.ServerConfig;
import io.trino.server.protocol.QueryDataProducerFactory;
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.spool.SpoolingManagerContext;
import io.trino.spi.spool.SpoolingManagerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.deserialize;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.serialize;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.STORAGE;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.spi.spool.SpooledLocation.coordinatorLocation;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static org.assertj.core.api.Assertions.assertThat;

class TestSpoolingQueryDataProducer
{
    private static final Map<String, List<String>> HEADERS = Map.of("encryption-key", List.of("secret"));
    private static final Map<String, List<String>> SIGNED_HEADERS = Map.of("encryption-key", List.of("secret"), "signed-header", List.of("value"));
    private static final SpooledSegmentHandle HANDLE = new SpooledSegmentHandle()
    {
        @Override
        public String identifier()
        {
            return "segment";
        }

        @Override
        public String encoding()
        {
            return "json";
        }

        @Override
        public Instant expirationTime()
        {
            return Instant.parse("2026-01-02T00:00:00Z");
        }
    };

    @Test
    void testInlineSegmentsContainOnlyTheirOwnBytes()
    {
        byte[] first = "first-segment".getBytes(UTF_8);
        byte[] second = "second-segment".getBytes(UTF_8);

        DataAttributes firstAttributes = DataAttributes.builder()
                .set(ROWS_COUNT, 1L)
                .set(SEGMENT_SIZE, first.length)
                .build();

        DataAttributes secondAttributes = DataAttributes.builder()
                .set(ROWS_COUNT, 1L)
                .set(SEGMENT_SIZE, second.length)
                .build();

        var page = serialize(List.of(
                SpooledMetadataBlock.forInlineData(firstAttributes, Slices.wrappedBuffer(first)),
                SpooledMetadataBlock.forInlineData(secondAttributes, Slices.wrappedBuffer(second))));

        List<SpooledMetadataBlock> decoded = deserialize(page);

        assertThat(decoded).hasSize(2);
        assertThat(((SpooledMetadataBlock.Inlined) decoded.get(0)).data().byteArray().length)
                .isGreaterThan(first.length);

        QueryResultRows rows = queryResultRowsBuilder()
                .withTypes(List.of(VARBINARY))
                .addPage(page)
                .build();

        ExternalUriInfo externalUriInfo = new ExternalUriInfo(MockUriInfo.from("http://localhost:8080"), "");

        EncodedQueryData result = (EncodedQueryData) new SpoolingQueryDataProducer("json", new TestingSpoolingManager(), STORAGE)
                .produce(externalUriInfo, rows, _ -> {});

        assertThat(result.getSegments()).hasSize(2);

        InlineSegment firstSegment = (InlineSegment) result.getSegments().get(0);
        InlineSegment secondSegment = (InlineSegment) result.getSegments().get(1);

        assertThat(firstSegment.getData()).isEqualTo(first);
        assertThat(secondSegment.getData()).isEqualTo(second);

        assertThat(firstSegment.getOffset()).isZero();
        assertThat(secondSegment.getOffset()).isEqualTo(1);
    }

    @Test
    void testCoordinatorFactoryWithSpoolingEnabled()
            throws IOException
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .addProperty("protocol.spooling.enabled", "true")
                .addProperty("protocol.spooling.shared-secret-key", getEncoder().encodeToString(new byte[32]))
                .build()) {
            QueryDataProducerFactory factory = server.getInstance(Key.get(QueryDataProducerFactory.class));
            assertThat(factory.create(testSessionBuilder().setQueryDataEncoding(Optional.of("json")).build(), List.of(VARBINARY)))
                    .isInstanceOf(SpoolingQueryDataProducer.class);
        }
    }

    @ParameterizedTest
    @EnumSource(SegmentRetrievalMode.class)
    void testSigningOnCoordinator(SegmentRetrievalMode mode)
            throws IOException
    {
        TestingSpoolingManager workerManager = new TestingSpoolingManager();
        SpoolingManager worker = bridge(workerManager, mode);
        SpooledLocation location = worker.location(HANDLE);
        assertThat(location).isInstanceOf(SpooledLocation.CoordinatorLocation.class);
        assertThat(workerManager.signatures).isZero();

        // Deliver the worker metadata later, with separate coordinator state and a new signing time.
        TestingSpoolingManager coordinatorManager = new TestingSpoolingManager();
        coordinatorManager.now = Instant.parse("2026-01-01T02:00:00Z");
        SpoolingManager coordinator = bridge(coordinatorManager, mode);
        SpoolingQueryDataProducer producer = new SpoolingQueryDataProducer("json", coordinator, mode);
        EncodedQueryData result = (EncodedQueryData) producer.produce(uriInfo(), rows(location), exception -> {
            throw exception;
        });

        SpooledSegment segment = (SpooledSegment) result.getSegments().getFirst();
        assertThat(segment.getAckUri()).isEqualTo(URI.create("http://localhost:8080/v1/spooled/ack/" + location.identifier().toStringUtf8()));
        assertThat(segment.getOffset()).isZero();
        if (mode == STORAGE) {
            assertThat(coordinatorManager.signatures).isEqualTo(1);
            assertThat(segment.getDataUri()).isEqualTo(URI.create("https://storage/segment?expires=" + coordinatorManager.now.plusSeconds(3600).getEpochSecond()));
            assertThat(segment.getHeaders()).isEqualTo(SIGNED_HEADERS);
        }
        else {
            assertThat(coordinatorManager.signatures).isZero();
            assertThat(segment.getDataUri()).isEqualTo(URI.create("http://localhost:8080/v1/spooled/download/" + location.identifier().toStringUtf8()));
            assertThat(segment.getHeaders()).isEqualTo(HEADERS);
        }
        // The acknowledgement identifier remains encrypted and can be decoded with the returned headers.
        assertThat(coordinator.handle(location.identifier(), segment.getHeaders())).isEqualTo(HANDLE);
        assertThat(workerManager.signatures).isZero();
    }

    @Test
    void testSigningFailureIsReported()
            throws IOException
    {
        TestingSpoolingManager manager = new TestingSpoolingManager();
        manager.signingFailure = Optional.of(new IOException("Signing failed"));
        SpoolingManager coordinator = bridge(manager, STORAGE);
        List<TrinoException> failures = new ArrayList<>();
        var result = new SpoolingQueryDataProducer("json", coordinator, STORAGE)
                .produce(uriInfo(), rows(coordinator.location(HANDLE)), failures::add);
        assertThat(result).isNull();
        assertThat(failures).singleElement().satisfies(exception -> {
            assertThat(exception.getErrorCode()).isEqualTo(SERIALIZATION_ERROR.toErrorCode());
            assertThat(exception).hasCauseInstanceOf(IOException.class);
        });
    }

    @Test
    void testMissingDirectLocationIsReported()
            throws IOException
    {
        TestingSpoolingManager manager = new TestingSpoolingManager();
        manager.directAccessSupported = false;
        SpoolingManager coordinator = bridge(manager, STORAGE);
        List<TrinoException> failures = new ArrayList<>();
        var result = new SpoolingQueryDataProducer("json", coordinator, STORAGE)
                .produce(uriInfo(), rows(coordinator.location(HANDLE)), failures::add);
        assertThat(result).isNull();
        assertThat(failures).singleElement().satisfies(exception ->
                assertThat(exception).hasRootCauseMessage("Could not generate pre-signed URI"));
    }

    private static ExternalUriInfo uriInfo()
    {
        return new ExternalUriInfo(MockUriInfo.from("http://localhost:8080"), "");
    }

    private static QueryResultRows rows(SpooledLocation location)
    {
        return queryResultRowsBuilder()
                .withTypes(List.of(VARBINARY))
                .addPage(serialize(SpooledMetadataBlock.forSpooledLocation(location, DataAttributes.builder()
                        .set(ROWS_COUNT, 1L)
                        .set(SEGMENT_SIZE, 10)
                        .build())))
                .build();
    }

    private static SpoolingManager bridge(SpoolingManager manager, SegmentRetrievalMode mode)
    {
        OpenTelemetry telemetry = OpenTelemetry.noop();
        SpoolingManagerRegistry registry = new SpoolingManagerRegistry(
                new InternalNode("test", URI.create("http://localhost:8080"), NodeVersion.UNKNOWN, true),
                new ServerConfig(),
                new SpoolingEnabledConfig().setEnabled(true),
                telemetry,
                telemetry.getTracer("test"));
        registry.loadSpoolingManager(new SpoolingManagerFactory()
        {
            @Override
            public String getName()
            {
                return "test";
            }

            @Override
            public SpoolingManager create(Map<String, String> config, SpoolingManagerContext context)
            {
                return manager;
            }
        }, Map.of());
        return new SpoolingManagerBridge(new SpoolingConfig()
                .setRetrievalMode(mode)
                .setSharedSecretKey(getEncoder().encodeToString(new byte[32])), registry);
    }

    private static class TestingSpoolingManager
            implements SpoolingManager
    {
        private int signatures;
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");
        private Optional<IOException> signingFailure = Optional.empty();
        private boolean directAccessSupported = true;

        @Override
        public SpooledSegmentHandle create(SpoolingContext context)
        {
            return HANDLE;
        }

        @Override
        public OutputStream createOutputStream(SpooledSegmentHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream openInputStream(SpooledSegmentHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(SpooledSegmentHandle handle) {}

        @Override
        public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
                throws IOException
        {
            if (signingFailure.isPresent()) {
                throw signingFailure.orElseThrow();
            }
            if (!directAccessSupported) {
                return Optional.empty();
            }
            assertThat(handle).isEqualTo(HANDLE);
            signatures++;
            return Optional.of(new DirectLocation(utf8Slice(handle.identifier()), URI.create("https://storage/segment?expires=" + now.plusSeconds(3600).getEpochSecond()), SIGNED_HEADERS));
        }

        @Override
        public SpooledLocation location(SpooledSegmentHandle handle)
        {
            return coordinatorLocation(utf8Slice(handle.identifier()), HEADERS);
        }

        @Override
        public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
        {
            assertThat(identifier.toStringUtf8()).isEqualTo(HANDLE.identifier());
            assertThat(headers).containsAllEntriesOf(HEADERS);
            return HANDLE;
        }

        @Override
        public boolean isRecoverableException(IOException exception)
        {
            return false;
        }
    }
}
