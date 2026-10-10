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

import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.QueryResultRows;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.deserialize;
import static io.trino.server.protocol.spooling.SpooledMetadataBlockSerde.serialize;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;

class TestSpoolingQueryDataProducer
{
    @Test
    void testInlineSegmentsContainOnlyTheirOwnBytes()
    {
        byte[] first = utf8Slice("first-segment").getBytes();
        byte[] second = utf8Slice("second-segment").getBytes();

        DataAttributes firstAttributes = DataAttributes.builder()
                .set(ROWS_COUNT, 1L)
                .set(SEGMENT_SIZE, first.length)
                .build();

        DataAttributes secondAttributes = DataAttributes.builder()
                .set(ROWS_COUNT, 1L)
                .set(SEGMENT_SIZE, second.length)
                .build();

        var page = serialize(List.of(
                SpooledMetadataBlock.forInlineData(firstAttributes, utf8Slice("first-segment")),
                SpooledMetadataBlock.forInlineData(secondAttributes, utf8Slice("second-segment"))));

        List<SpooledMetadataBlock> decoded = deserialize(page);

        assertThat(decoded).hasSize(2);
        assertThat(((SpooledMetadataBlock.Inlined) decoded.get(0)).data().byteArray().length)
                .isGreaterThan(first.length);

        QueryResultRows rows = queryResultRowsBuilder()
                .withTypes(List.of(VARBINARY))
                .addPage(page)
                .build();

        UriInfo uriInfo = proxy(UriInfo.class, (method, _) -> {
            if (method.equals("getBaseUriBuilder")) {
                return UriBuilder.fromUri("http://localhost:8080");
            }
            return null;
        });

        HttpHeaders headers = proxy(HttpHeaders.class, (_, _) -> null);

        ExternalUriInfo externalUriInfo = new ExternalUriInfo(uriInfo, headers);

        EncodedQueryData result = (EncodedQueryData) new SpoolingQueryDataProducer("json")
                .produce(externalUriInfo, rows, _ -> {});

        assertThat(result).isNotNull();
        assertThat(result.getSegments()).hasSize(2);

        InlineSegment firstSegment = (InlineSegment) result.getSegments().get(0);
        InlineSegment secondSegment = (InlineSegment) result.getSegments().get(1);

        assertThat(firstSegment.getData()).isEqualTo(first);
        assertThat(secondSegment.getData()).isEqualTo(second);

        assertThat(firstSegment.getOffset()).isZero();
        assertThat(secondSegment.getOffset()).isEqualTo(1);
    }

    private static <T> T proxy(Class<T> type, Invocation invocation)
    {
        Object instance = Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (_, method, arguments) -> invocation.invoke(method.getName(), arguments));

        return type.cast(instance);
    }

    @FunctionalInterface
    private interface Invocation
    {
        Object invoke(String method, Object[] arguments);
    }
}
