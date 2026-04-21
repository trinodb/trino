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
package io.trino.client.spooling;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.client.QueryData;
import io.trino.client.TrinoJsonCodec;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.DataAttribute.SCHEMA;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpoolingDataSerialization
{
    private static final TrinoJsonCodec<QueryData> QUERY_DATA_CODEC = jsonCodec(QueryData.class);
    private static final TrinoJsonCodec<Segment> SEGMENT_CODEC = jsonCodec(Segment.class);

    @Test
    void testEncodedDataSerializationRoundTrip()
            throws JsonProcessingException
    {
        DataAttributes attributes = DataAttributes.builder()
                .set(SCHEMA, "<schema>")
                .build();

        QueryData data = new EncodedQueryData("json", attributes, ImmutableList.of(inlineSegment(), spooledSegment()));
        assertThat(QUERY_DATA_CODEC.fromJson(QUERY_DATA_CODEC.toJson(data)))
                .isEqualTo(data);

        assertThat(QUERY_DATA_CODEC.toJson(data))
                .isEqualTo(format("{\"encoding\":\"json\",\"metadata\":{\"schema\":\"<schema>\"},\"segments\":[%s,%s]}", inlineSegmentSerialized(), spooledSegmentSerialized()));
    }

    @Test
    void testInlineSegmentSerializationRoundTrip()
            throws JsonProcessingException
    {
        assertThat(SEGMENT_CODEC.fromJson(SEGMENT_CODEC.toJson(inlineSegment())))
                .isEqualTo(inlineSegment());

        assertThat(SEGMENT_CODEC.toJson(inlineSegment()))
                .isEqualTo(inlineSegmentSerialized());

        assertThat(SEGMENT_CODEC.fromJson(inlineSegmentSerialized()))
                .isEqualTo(inlineSegment());
    }

    @Test
    void testSpooledSegmentSerializationRoundTrip()
            throws JsonProcessingException
    {
        assertThat(SEGMENT_CODEC.fromJson(SEGMENT_CODEC.toJson(spooledSegment())))
                .isEqualTo(spooledSegment());

        assertThat(SEGMENT_CODEC.toJson(spooledSegment()))
                .isEqualTo(spooledSegmentSerialized());

        assertThat(SEGMENT_CODEC.fromJson(spooledSegmentSerialized()))
                .isEqualTo(spooledSegment());
    }

    private static Segment inlineSegment()
    {
        DataAttributes attributes = DataAttributes.builder()
                .set(SEGMENT_SIZE, 1000)
                .set(ROW_OFFSET, 0L)
                .set(ROWS_COUNT, 0L)
                .build();
        return new InlineSegment("testData".getBytes(StandardCharsets.UTF_8), attributes);
    }

    private static String inlineSegmentSerialized()
    {
        return "{\"type\":\"inline\",\"data\":\"dGVzdERhdGE=\",\"metadata\":{\"segmentSize\":1000,\"rowOffset\":0,\"rowsCount\":0}}";
    }

    private static Segment spooledSegment()
    {
        DataAttributes attributes = DataAttributes.builder()
                .set(SEGMENT_SIZE, 1000)
                .set(ROW_OFFSET, 0L)
                .set(ROWS_COUNT, 0L)
                .build();
        return new SpooledSegment(URI.create("https://download.uri"), URI.create("https://ack.uri"), attributes, ImmutableMap.of("X-Trino-Header", ImmutableList.of("Header-Value")));
    }

    private static String spooledSegmentSerialized()
    {
        return "{\"type\":\"spooled\",\"uri\":\"https://download.uri\",\"ackUri\":\"https://ack.uri\",\"metadata\":{\"segmentSize\":1000,\"rowOffset\":0,\"rowsCount\":0},\"headers\":{\"X-Trino-Header\":[\"Header-Value\"]}}";
    }
}
