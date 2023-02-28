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
package io.trino.filesystem;

import com.google.common.collect.ImmutableList;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public record FileEntry(String location, long length, Instant lastModified, Optional<List<Block>> blocks)
{
    public FileEntry
    {
        checkArgument(length >= 0, "length is negative");
        requireNonNull(location, "location is null");
        requireNonNull(blocks, "blocks is null");
        blocks = blocks.map(locations -> validatedBlocks(locations, length));
    }

    public record Block(List<String> hosts, long offset, long length)
    {
        public Block
        {
            hosts = ImmutableList.copyOf(requireNonNull(hosts, "hosts is null"));
            checkArgument(offset >= 0, "offset is negative");
            checkArgument(length >= 0, "length is negative");
        }
    }

    private static List<Block> validatedBlocks(List<Block> blocks, long length)
    {
        checkArgument(!blocks.isEmpty(), "blocks is empty");
        blocks = blocks.stream()
                .sorted(comparing(Block::offset))
                .collect(toImmutableList());

        long position = 0;
        for (Block block : blocks) {
            checkArgument(block.offset() <= position, "blocks have a gap");
            position = max(position, addExact(block.offset(), block.length()));
        }
        checkArgument(position >= length, "blocks do not cover file");

        return blocks;
    }
}
