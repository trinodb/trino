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
package io.trino.type;

import io.airlift.slice.DynamicSliceOutput;
import io.trino.json.Json;
import io.trino.json.JsonBlock;
import io.trino.json.JsonBlockBuilder;
import io.trino.json.JsonBlockEncoding;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItems;
import io.trino.json.JsonObject;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonBlock
{
    @Test
    void testAppendAndRead()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        Json a = JsonItems.parseToTree(utf8Slice("{\"k\": 1}"));
        Json b = JsonItems.parseToTree(utf8Slice("[1, 2, 3]"));
        Json c = JsonItemBuilder.JSON_NULL;
        builder.appendJson(a);
        builder.appendJson(b);
        builder.appendNull();
        builder.appendJson(c);

        Block block = builder.build();
        assertThat(block).isInstanceOf(JsonBlock.class);
        JsonBlock jsonBlock = (JsonBlock) block;
        assertThat(jsonBlock.getPositionCount()).isEqualTo(4);
        assertThat(jsonBlock.isNull(0)).isFalse();
        assertThat(jsonBlock.isNull(2)).isTrue();
        // The block hands back a view over its own buffer, equal to what was written.
        assertThat(jsonBlock.getJson(0)).isEqualTo(a);
        assertThat(jsonBlock.getJson(1)).isEqualTo(b);
        assertThat(jsonBlock.getJson(3)).isEqualTo(c);
    }

    @Test
    void testAllNullsBuildsRle()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        builder.appendNull();
        builder.appendNull();
        builder.appendNull();
        Block block = builder.build();
        // An all-null block builds as a RunLengthEncodedBlock — same pattern as the
        // primitive array block builders.
        assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(3);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    void testCopyRegion()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        Json a = JsonItems.parseToTree(utf8Slice("1"));
        Json b = JsonItems.parseToTree(utf8Slice("2"));
        Json c = JsonItems.parseToTree(utf8Slice("3"));
        Json d = JsonItems.parseToTree(utf8Slice("4"));
        builder.appendJson(a);
        builder.appendJson(b);
        builder.appendJson(c);
        builder.appendJson(d);

        JsonBlock region = ((JsonBlock) builder.build()).copyRegion(1, 2);
        assertThat(region.getPositionCount()).isEqualTo(2);
        assertThat(region.getJson(0)).isEqualTo(b);
        assertThat(region.getJson(1)).isEqualTo(c);
    }

    @Test
    void testSerdeRoundTrip()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 3);
        Json a = JsonItems.parseToTree(utf8Slice("{\"k\": [1, 2, 3]}"));
        Json b = JsonItems.parseToTree(utf8Slice("\"hello\""));
        builder.appendJson(a);
        builder.appendNull();
        builder.appendJson(b);
        JsonBlock built = builder.buildValueBlock();

        JsonBlockEncoding encoding = new JsonBlockEncoding();
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        encoding.writeBlock(null, out, built);

        JsonBlock read = encoding.readBlock(null, out.slice().getInput());
        assertThat(read.getPositionCount()).isEqualTo(3);
        assertThat(read.isNull(1)).isTrue();
        // After serde, values come back as EncodedJson over the deserialized slice — not
        // the original references. Compare by canonical encoding.
        assertThat(read.getJson(0).encoding()).isEqualTo(a.encoding());
        assertThat(read.getJson(2).encoding()).isEqualTo(b.encoding());
    }

    @Test
    void testInvalidPositionThrows()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 1);
        builder.appendJson(JsonItemBuilder.JSON_NULL);
        JsonBlock block = builder.buildValueBlock();
        assertThatThrownBy(() -> block.getJson(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid position -1");
        assertThatThrownBy(() -> block.getJson(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid position 1");
    }

    @Test
    void testSizeAccountsForPayload()
    {
        // Operators size memory reservations and spill from these accessors, so a large payload
        // has to show up in them. A flat block stores bytes, so a tree value is encoded when it is
        // written — the cost the reference-holding block deferred — and the sizes are then O(1).
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 1);
        JsonObject tree = (JsonObject) JsonItems.parseToTree(utf8Slice("{\"deep\": {\"deeper\": [1, 2, 3]}}"));
        builder.appendJson(tree);
        ValueBlock block = builder.buildValueBlock();

        long size = block.getSizeInBytes();
        long retained = block.getRetainedSizeInBytes();
        assertThat(size).isGreaterThan(0);
        assertThat(retained).isGreaterThan(size);

        // A big value must not report the same size as a small one.
        JsonBlockBuilder bigBuilder = new JsonBlockBuilder(null, 1);
        bigBuilder.appendJson(JsonItemBuilder.encodeVarchar(utf8Slice("x".repeat(100_000))));
        ValueBlock big = bigBuilder.buildValueBlock();

        JsonBlockBuilder smallBuilder = new JsonBlockBuilder(null, 1);
        smallBuilder.appendJson(JsonItemBuilder.encodeVarchar(utf8Slice("x")));
        ValueBlock small = smallBuilder.buildValueBlock();

        assertThat(big.getSizeInBytes()).isGreaterThan(100_000);
        assertThat(big.getSizeInBytes()).isGreaterThan(small.getSizeInBytes() * 100);
        assertThat(big.getRetainedSizeInBytes()).isGreaterThan(100_000);
    }

    @Test
    void testAppendAfterResetOverNull()
    {
        // Rewinding over a null and appending a value must not leave the position null. The
        // retained prefix keeps a null (position 0) so the block still carries a null array —
        // otherwise a stale marker at position 1 would be masked by dropping the array.
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        Json value = JsonItems.parseToTree(utf8Slice("{\"k\": 1}"));

        builder.appendNull();
        builder.appendNull();
        builder.resetTo(1);
        builder.appendJson(value);

        JsonBlock block = builder.buildValueBlock();
        assertThat(block.getPositionCount()).isEqualTo(2);
        assertThat(block.isNull(0)).isTrue();
        assertThat(block.isNull(1)).isFalse();
        assertThat(block.getJson(1)).isEqualTo(value);

        // Rewinding away the only non-null value must leave an all-null prefix, which builds
        // as the all-null encoding (two positions, so the RLE isn't collapsed to its value).
        JsonBlockBuilder nulls = new JsonBlockBuilder(null, 4);
        nulls.appendNull();
        nulls.appendNull();
        nulls.appendJson(value);
        nulls.resetTo(2);
        assertThat(nulls.build()).isInstanceOf(RunLengthEncodedBlock.class);
    }

    @Test
    void testValidityBitmap()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, 4);
        Json a = JsonItems.parseToTree(utf8Slice("{\"k\": 1}"));
        Json b = JsonItems.parseToTree(utf8Slice("[1, 2, 3]"));
        builder.appendJson(a);
        builder.appendNull();
        builder.appendJson(b);
        builder.appendNull();
        JsonBlock block = builder.buildValueBlock();

        Bitmap bitmap = block.getValidityBitmap().orElseThrow();
        assertThat(bitmap.getBitCount()).isEqualTo(4);
        // Set bit means valid (non-null) — the inverse of valueIsNull.
        assertThat(bitmap.isSet(0)).isTrue();
        assertThat(bitmap.isSet(1)).isFalse();
        assertThat(bitmap.isSet(2)).isTrue();
        assertThat(bitmap.isSet(3)).isFalse();

        // A region-sliced block carries a non-zero arrayOffset: the bitmap must describe the
        // region, not the backing array. Positions 1..3 of the block above are null, b, null.
        JsonBlock region = (JsonBlock) block.getRegion(1, 3);
        Bitmap regionBitmap = region.getValidityBitmap().orElseThrow();
        assertThat(regionBitmap.getBitCount()).isEqualTo(3);
        assertThat(regionBitmap.isSet(0)).isFalse();
        assertThat(regionBitmap.isSet(1)).isTrue();
        assertThat(regionBitmap.isSet(2)).isFalse();

        // No nulls at all — nothing to describe.
        JsonBlockBuilder nonNull = new JsonBlockBuilder(null, 2);
        nonNull.appendJson(a);
        nonNull.appendJson(b);
        assertThat(nonNull.buildValueBlock().getValidityBitmap()).isEmpty();
    }
}
