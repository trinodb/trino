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
package io.trino.sql.newir;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.newir.FormatOptions.INDENT;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * A Region is a list of Blocks forming a Control Flow Graph.
 * A Region is attached to an Operation, and an Operation can have multiple Regions forming an ordered list.
 * Unlike Blocks, Regions do not have an optional name (label). They can be accessed by their position in the list.
 * The semantics of a Region is defined by the enclosing Operation. It means that control flow across Regions
 * is implicitly managed by the enclosing Operation.
 * <p>
 * For now, we only support single-block Regions.
 */
public record Region(List<Block> blocks)
{
    public Region(List<Block> blocks)
    {
        this.blocks = ImmutableList.copyOf(requireNonNull(blocks, "blocks is null"));
        if (blocks.size() != 1) { // TODO when we lift the single block restriction, verify that the blocks list is not empty
            throw new TrinoException(IR_ERROR, "expected 1 block, actual: " + blocks.size());
        }
    }

    public static Region singleBlockRegion(Block block)
    {
        return new Region(ImmutableList.of(block));
    }

    public Block getOnlyBlock()
    {
        if (blocks().size() != 1) {
            throw new TrinoException(IR_ERROR, "expected 1 block, actual: " + blocks.size());
        }

        return getOnlyElement(blocks());
    }

    public String print(int version, int indentLevel, FormatOptions formatOptions)
    {
        String indent = INDENT.repeat(indentLevel);

        return blocks().stream()
                .map(block -> block.print(version, indentLevel, formatOptions))
                .collect(joining("\n", "{\n", "\n" + indent + "}"));
    }

    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return print(1, indentLevel, formatOptions);
    }
}
