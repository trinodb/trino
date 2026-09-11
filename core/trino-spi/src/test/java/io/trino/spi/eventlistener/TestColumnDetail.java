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

import java.util.Set;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnDetail
{
    private final JsonCodec<ColumnDetail> codec = jsonCodec(ColumnDetail.class);

    @Test
    public void testConvenienceConstructorHasEmptySubtypes()
    {
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col");
        assertThat(detail.getTransformationTypes()).isEmpty();
    }

    @Test
    public void testDeserializesOldPayloadWithoutTransformationTypes()
    {
        // A ColumnDetail emitted before transformationTypes existed has no such key; it must deserialize to
        // an empty set rather than fail, preserving backward compatibility for existing event consumers.
        ColumnDetail detail = codec.fromJson("{\"catalog\":\"c\",\"schema\":\"s\",\"table\":\"t\",\"columnName\":\"col\"}");
        assertThat(detail.getTransformationTypes()).isEmpty();
        assertThat(detail).isEqualTo(new ColumnDetail("c", "s", "t", "col"));
    }

    @Test
    public void testJsonRoundTripPreservesTransformationTypes()
    {
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col", Set.of(ColumnTransformationType.AGGREGATION));
        assertThat(codec.fromJson(codec.toJson(detail)).getTransformationTypes()).containsExactly(ColumnTransformationType.AGGREGATION);
    }

    @Test
    public void testJsonRoundTripPreservesMultipleTransformationTypes()
    {
        // A source column can reach an output through several paths (for example the branches of a UNION),
        // so every distinct subtype must survive the round trip.
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col", Set.of(ColumnTransformationType.IDENTITY, ColumnTransformationType.AGGREGATION));
        assertThat(codec.fromJson(codec.toJson(detail)).getTransformationTypes())
                .containsExactlyInAnyOrder(ColumnTransformationType.IDENTITY, ColumnTransformationType.AGGREGATION);
    }

    @Test
    public void testEmptySubtypesOmittedFromJson()
    {
        // Most columns carry no subtype; the key must be omitted so events stay lean and match pre-feature payloads.
        ColumnDetail detail = new ColumnDetail("c", "s", "t", "col");
        assertThat(codec.toJson(detail)).doesNotContain("transformationTypes");
    }

    @Test
    public void testSubtypesExcludedFromEqualsAndHashCode()
    {
        ColumnDetail withIdentity = new ColumnDetail("c", "s", "t", "col", Set.of(ColumnTransformationType.IDENTITY));
        ColumnDetail withAggregation = new ColumnDetail("c", "s", "t", "col", Set.of(ColumnTransformationType.AGGREGATION));
        assertThat(withIdentity).isEqualTo(withAggregation);
        assertThat(withIdentity.hashCode()).isEqualTo(withAggregation.hashCode());
        assertThat(withAggregation.getTransformationTypes()).containsExactly(ColumnTransformationType.AGGREGATION);
    }
}
