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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.parquet.reader.ShreddedValue.ShreddedArray;
import io.trino.parquet.reader.ShreddedValue.ShreddedObject;
import io.trino.parquet.reader.ShreddedValue.ShreddedScalar;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariantShreddingReconstructor
{
    @Test
    void testScalarFromTypedValue()
    {
        ShreddedValue node = new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(5)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(5L);
    }

    @Test
    void testScalarFallsBackToValueWhenNotShredded()
    {
        Variant string = Variant.ofString("hi");
        ShreddedValue node = new ShreddedScalar(Optional.of(string.data()), Optional.empty());
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo("hi");
    }

    @Test
    void testMissingScalarReturnsEmpty()
    {
        ShreddedValue node = new ShreddedScalar(Optional.empty(), Optional.empty());
        assertThat(VariantShreddingReconstructor.reconstruct(node, EMPTY_METADATA)).isEmpty();
    }

    @Test
    void testFullyShreddedObject()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("x"))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("a", 1L, "b", "x"));
    }

    @Test
    void testPartiallyShreddedObjectMergesValueFields()
    {
        // value holds the non-shredded field c; the shredded field a comes from typed_value
        Variant partialValue = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        ShreddedValue node = new ShreddedObject(
                Optional.of(partialValue.data()),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))))));
        assertThat(reconstruct(node, partialValue.metadata())).isEqualTo(ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testObjectOmitsMissingShreddedField()
    {
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("a"), new ShreddedScalar(Optional.empty(), Optional.empty()),
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(2))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("b", 2L));
    }

    @Test
    void testNestedObject()
    {
        ShreddedValue inner = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        utf8Slice("b"), new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))))));
        ShreddedValue node = new ShreddedObject(
                Optional.empty(),
                Optional.of(ImmutableMap.of(utf8Slice("a"), inner)));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableMap.of("a", ImmutableMap.of("b", 1L)));
    }

    @Test
    void testShreddedArray()
    {
        ShreddedValue node = new ShreddedArray(
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofLong(1))),
                        new ShreddedScalar(Optional.empty(), Optional.of(Variant.ofString("x"))))));
        assertThat(reconstruct(node, EMPTY_METADATA)).isEqualTo(ImmutableList.of(1L, "x"));
    }

    @Test
    void testArrayFallsBackToValueWhenNotShredded()
    {
        Variant array = Variant.ofArray(ImmutableList.of(Variant.ofLong(1), Variant.ofLong(2)));
        ShreddedValue node = new ShreddedArray(Optional.of(array.data()), Optional.empty());
        assertThat(reconstruct(node, array.metadata())).isEqualTo(ImmutableList.of(1L, 2L));
    }

    private static Object reconstruct(ShreddedValue node, Metadata metadata)
    {
        return VariantShreddingReconstructor.reconstruct(node, metadata).orElseThrow().toObject();
    }
}
