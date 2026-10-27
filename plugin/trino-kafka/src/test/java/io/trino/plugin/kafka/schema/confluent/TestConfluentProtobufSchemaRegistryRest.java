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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.encoder.protobuf.ConfluentProtobufRowEncoderFactory;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoder;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.frame;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.readFrame;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Exercises the protobuf schema provider through the schema registry REST client against an
 * in-process registry, including HTTP basic authentication and reference resolution over REST.
 */
@TestInstance(PER_CLASS)
public class TestConfluentProtobufSchemaRegistryRest
{
    private static final String USER_INFO = "trino:secret";
    private static final String ADDRESS_PROTO =
            """
            syntax = "proto3";
            package base;
            message Address {
                string city = 1;
            }
            """;
    private static final String ORDER_PROTO =
            """
            syntax = "proto3";
            import "address.proto";
            message schema {
                string col_1 = 1;
                base.Address address = 2;
            }
            """;
    private static final int ADDRESS_ID = 1;
    private static final int ORDER_ID = 2;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private String baseUrl;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        baseUrl = "127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    public void tearDown()
    {
        server.stop(0);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        String path = exchange.getRequestURI().getPath();
        requests.add(path);
        String expectedAuthorization = "Basic " + Base64.getEncoder().encodeToString(USER_INFO.getBytes(UTF_8));
        if (!expectedAuthorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            respond(exchange, 401, Map.of("error_code", 401, "message", "Unauthorized"));
            return;
        }
        Map<String, Object> addressReference = Map.of("name", "address.proto", "subject", "address-value", "version", 1);
        switch (path) {
            case "/schemas/ids/" + ADDRESS_ID -> respond(exchange, 200, Map.of("schemaType", "PROTOBUF", "schema", ADDRESS_PROTO));
            case "/schemas/ids/" + ORDER_ID -> respond(exchange, 200, Map.of("schemaType", "PROTOBUF", "schema", ORDER_PROTO, "references", List.of(addressReference)));
            case "/subjects/address-value/versions/1" -> respond(exchange, 200, Map.of("subject", "address-value", "version", 1, "id", ADDRESS_ID, "schemaType", "PROTOBUF", "schema", ADDRESS_PROTO));
            case "/subjects/orders-value/versions/latest" -> respond(exchange, 200, Map.of("subject", "orders-value", "version", 3, "id", ORDER_ID, "schemaType", "PROTOBUF", "schema", ORDER_PROTO, "references", List.of(addressReference)));
            case "/schemas/ids/3" -> respond(exchange, 200, Map.of("schemaType", "PROTOBUF", "schema", "syntax = \"proto3\"; import \"missing.proto\"; message schema {}", "references", List.of(Map.of("name", "missing.proto", "subject", "missing-value", "version", 1))));
            default -> respond(exchange, 404, Map.of("error_code", 40403, "message", "Schema not found"));
        }
    }

    private void respond(HttpExchange exchange, int status, Map<String, Object> body)
            throws IOException
    {
        byte[] bytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Test
    public void testDecodeWithReferencesOverRest()
    {
        SchemaRegistryClient client = createClient("http://" + USER_INFO + "@" + baseUrl, ImmutableMap.of("basic.auth.credentials.source", "URL"));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        requests.clear();

        DynamicMessage decoded = provider.parseDynamicMessage(frame(ORDER_ID, ImmutableList.of(0), orderBytes()));
        Descriptor descriptor = decoded.getDescriptorForType();
        assertThat(descriptor.getName()).isEqualTo("schema");
        assertThat(decoded.getField(descriptor.findFieldByName("col_1"))).isEqualTo("Trino");
        DynamicMessage address = (DynamicMessage) decoded.getField(descriptor.findFieldByName("address"));
        assertThat(address.getField(address.getDescriptorForType().findFieldByName("city"))).isEqualTo("Boston");
        assertThat(requests).containsExactly("/schemas/ids/" + ORDER_ID, "/subjects/address-value/versions/1");

        // the descriptor is cached, so no further requests are made
        provider.parseDynamicMessage(frame(ORDER_ID, ImmutableList.of(0), orderBytes()));
        assertThat(requests).hasSize(2);
    }

    @Test
    public void testEncodeWithLatestSchemaOverRest()
            throws Exception
    {
        SchemaRegistryClient client = createClient("http://" + USER_INFO + "@" + baseUrl, ImmutableMap.of("basic.auth.credentials.source", "URL"));
        KafkaColumnHandle column = new KafkaColumnHandle("col_1", VARCHAR, "col_1", null, null, false, false, false, Optional.empty());
        RowEncoderSpec spec = new RowEncoderSpec(ProtobufRowEncoder.NAME, Optional.empty(), Optional.of("orders-value"), ImmutableList.of(column), "orders", MESSAGE);
        try (RowEncoder encoder = new ConfluentProtobufRowEncoderFactory(client).create(TestingConnectorSession.SESSION, spec)) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            byte[] record = encoder.toByteArray();
            assertThat(readFrame(record).schemaId()).isEqualTo(ORDER_ID);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getField(decoded.getDescriptorForType().findFieldByName("col_1"))).isEqualTo("Trino");
        }
    }

    @Test
    public void testMissingCredentials()
    {
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(createClient("http://" + baseUrl, ImmutableMap.of()));
        assertTrinoException(() -> provider.parseDynamicMessage(frame(ORDER_ID, ImmutableList.of(0), orderBytes())), KAFKA_SCHEMA_REGISTRY_ERROR, "Failed to look up schema id 2 in the schema registry: Unauthorized; error code: 401");
    }

    @Test
    public void testUnknownSchemaId()
    {
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(createClient("http://" + USER_INFO + "@" + baseUrl, ImmutableMap.of("basic.auth.credentials.source", "URL")));
        assertTrinoException(() -> provider.parseDynamicMessage(frame(42, ImmutableList.of(0), new byte[0])), KAFKA_SCHEMA_ERROR, "Schema id 42 does not exist in the schema registry");
    }

    @Test
    public void testDanglingReferenceOverRest()
    {
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(createClient("http://" + USER_INFO + "@" + baseUrl, ImmutableMap.of("basic.auth.credentials.source", "URL")));
        assertTrinoException(() -> provider.parseDynamicMessage(frame(3, ImmutableList.of(0), new byte[0])), KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'missing.proto' points to missing-value:1, which does not exist in the schema registry");
    }

    @Test
    public void testRegistryUnavailable()
            throws IOException
    {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(createClient("http://127.0.0.1:" + unusedPort, ImmutableMap.of()));
        assertThatThrownBy(() -> provider.parseDynamicMessage(frame(ORDER_ID, ImmutableList.of(0), orderBytes())))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(KAFKA_SCHEMA_REGISTRY_ERROR.toErrorCode()))
                .hasMessageStartingWith("Failed to look up schema id 2 in the schema registry: ");
    }

    private static SchemaRegistryClient createClient(String url, Map<String, Object> properties)
    {
        return new CachedSchemaRegistryClient(
                ImmutableList.of(url),
                10,
                ImmutableList.of(new ConfluentProtobufSchemaProvider()),
                ImmutableMap.<String, Object>builder()
                        .putAll(properties)
                        .put("max.retries", "0")
                        .buildOrThrow());
    }

    private static byte[] orderBytes()
    {
        // field 1 (col_1) = "Trino", field 2 (address) = Address { field 1 (city) = "Boston" }
        return new byte[] {
                0x0A, 0x05, 'T', 'r', 'i', 'n', 'o',
                0x12, 0x08, 0x0A, 0x06, 'B', 'o', 's', 't', 'o', 'n',
        };
    }

    private static void assertTrinoException(Runnable runnable, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()))
                .hasMessage(message);
    }
}
