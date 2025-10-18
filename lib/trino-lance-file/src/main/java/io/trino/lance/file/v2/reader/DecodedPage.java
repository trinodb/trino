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
package io.trino.lance.file.v2.reader;

import io.trino.spi.block.Block;

import static java.util.Objects.requireNonNull;

public class DecodedPage
{
    private final Block block;
    private final RepetitionDefinitionUnraveler unraveler;

    public DecodedPage(Block block, RepetitionDefinitionUnraveler unraveler)
    {
        this.block = requireNonNull(block, "block is null");
        this.unraveler = requireNonNull(unraveler, "unraveler is null");
    }

    public Block getBlock()
    {
        return block;
    }

    public RepetitionDefinitionUnraveler getUnraveler()
    {
        return unraveler;
    }
}
