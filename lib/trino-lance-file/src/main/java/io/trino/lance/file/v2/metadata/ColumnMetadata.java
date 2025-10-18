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
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public record ColumnMetadata(int index, List<PageMetadata> pages, List<DiskRange> bufferOffsets)
{
    public ColumnMetadata
    {
        pages = ImmutableList.copyOf(pages);
        bufferOffsets = ImmutableList.copyOf(bufferOffsets);
    }

    public static ColumnMetadata from(int columnIndex, Slice data)
    {
        checkArgument(data != null, "data is null");

        build.buf.gen.lance.file.v2.ColumnMetadata proto;
        try {
            proto = build.buf.gen.lance.file.v2.ColumnMetadata.parseFrom(data.toByteBuffer());
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to fromProto ColumnMetadata proto: " + e);
        }

        List<PageMetadata> pages = proto.getPagesList().stream()
                .map(page -> {
                    long numRows = page.getLength();
                    long priority = page.getPriority();
                    int bufferCount = page.getBufferOffsetsList().size();
                    List<DiskRange> buffers = IntStream.range(0, bufferCount).boxed()
                            .map(i -> {
                                long position = page.getBufferOffsets(i);
                                return new DiskRange(position, page.getBufferSizes(i));
                            })
                            .collect(toImmutableList());
                    return new PageMetadata(numRows, priority, getPageLayout(page), buffers);
                })
                .collect(toImmutableList());

        int bufferCount = proto.getBufferOffsetsList().size();
        List<DiskRange> buffers = IntStream.range(0, bufferCount).boxed()
                .map(index -> {
                    long position = proto.getBufferOffsets(index);
                    return new DiskRange(position, proto.getBufferSizes(index));
                })
                .collect(toImmutableList());
        return new ColumnMetadata(columnIndex, pages, buffers);
    }

    private static PageLayout getPageLayout(build.buf.gen.lance.file.v2.ColumnMetadata.Page page)
    {
        checkArgument(page.hasEncoding(), "Page has no encoding");
        build.buf.gen.lance.file.v2.Encoding encoding = page.getEncoding();
        return switch (encoding.getLocationCase()) {
            case DIRECT -> {
                try {
                    Any any = Any.parseFrom(encoding.getDirect().getEncoding().toByteArray());
                    build.buf.gen.lance.encodings21.PageLayout layout = any.unpack(build.buf.gen.lance.encodings21.PageLayout.class);
                    yield PageLayout.fromProto(layout);
                }
                catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Failed to parse from proto message", e);
                }
            }
            case INDIRECT -> throw new UnsupportedOperationException("Indirect encoding not supported");
            default -> throw new UnsupportedOperationException("Invalid encoding: " + encoding);
        };
    }
}
