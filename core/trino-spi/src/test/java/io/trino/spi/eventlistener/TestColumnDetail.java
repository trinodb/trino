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
package io.trino.spi.eventlistener;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnDetail
{
    private final JsonCodec<ColumnDetail> codec = jsonCodec(ColumnDetail.class);

    @Test
    public void testConvenienceConstructorHasEmptySubtype()
    {
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col");
        assertThat(detail.getTransformationType()).isEmpty();
    }

    @Test
    public void testDeserializesOldPayloadWithoutTransformationType()
    {
        // A ColumnDetail emitted before transformationType existed has no such key; it must deserialize to
        // Optional.empty() rather than fail, preserving backward compatibility for existing event consumers.
        ColumnDetail detail = codec.fromJson("{\"catalog\":\"c\",\"schema\":\"s\",\"table\":\"t\",\"columnName\":\"col\"}");
        assertThat(detail.getTransformationType()).isEmpty();
        assertThat(detail).isEqualTo(new ColumnDetail("c", "s", "t", "col"));
    }

    @Test
    public void testJsonRoundTripPreservesTransformationType()
    {
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col", Optional.of(ColumnTransformationType.AGGREGATION));
        assertThat(codec.fromJson(codec.toJson(detail)).getTransformationType()).contains(ColumnTransformationType.AGGREGATION);
    }

    @Test
    public void testSubtypeExcludedFromEqualsAndHashCode()
    {
        ColumnDetail withIdentity = new ColumnDetail("c", "s", "t", "col", Optional.of(ColumnTransformationType.IDENTITY));
        ColumnDetail withAggregation = new ColumnDetail("c", "s", "t", "col", Optional.of(ColumnTransformationType.AGGREGATION));
        assertThat(withIdentity).isEqualTo(withAggregation);
        assertThat(withIdentity.hashCode()).isEqualTo(withAggregation.hashCode());
        assertThat(withAggregation.getTransformationType()).contains(ColumnTransformationType.AGGREGATION);
    }
}
