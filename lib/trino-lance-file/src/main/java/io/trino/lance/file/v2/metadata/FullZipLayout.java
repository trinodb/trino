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
package io.trino.lance.file.v2.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record FullZipLayout(
        int numRepBits,
        int numDeflBits,
        io.trino.lance.file.v2.metadata.FullZipLayout.Block block,
        int numItems,
        int numVisibleItems,
        List<RepDefLayer> repDefLayers)
        implements PageLayout
{
    public FullZipLayout
    {
        requireNonNull(block, "chunkSize is null");
        repDefLayers = ImmutableList.copyOf(repDefLayers);
    }

    public static FullZipLayout fromProto(build.buf.gen.lance.encodings21.FullZipLayout proto)
    {
        Block block = switch (proto.getDetailsCase()) {
            case BITS_PER_VALUE -> new Block.FixedWidthBlock(proto.getBitsPerValue());
            case BITS_PER_OFFSET -> new Block.VariableWidthBlock(proto.getBitsPerOffset());
            default -> throw new IllegalArgumentException("Unexpected details case: " + proto.getDetailsCase());
        };
        return new FullZipLayout(
                proto.getBitsRep(),
                proto.getBitsDef(),
                block,
                proto.getNumItems(),
                proto.getNumVisibleItems(),
                RepDefLayer.fromProtoList(proto.getLayersList()));
    }

    public sealed interface Block
            permits Block.FixedWidthBlock, Block.VariableWidthBlock
    {
        record FixedWidthBlock(int bitsPerValue)
                implements Block {}

        record VariableWidthBlock(int bitsPerOffset)
                implements Block {}
    }
}
