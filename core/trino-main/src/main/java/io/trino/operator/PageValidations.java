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
package io.trino.operator;

import com.google.common.base.Joiner;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class PageValidations
{
    private PageValidations() {}

    public static void validateOutputPageTypes(Page page, List<Type> expectedTypes, Supplier<String> debugContextSupplier)
    {
        if (page.getPositionCount() == 0) {
            // Ignore empty pages. It is valid for connectors/table functions to produce empty pages (with arbitrary block list)
            // interleaved with pages with data. Example of such behavior can be found in TableChangesFunctionProcessor (this is not exactly
            // related to SourcePage, but illustrates the contract, which is the same for Pages and SourcePages).
            // Empty pages are filtered out by Driver.
            return;
        }

        if (page.getChannelCount() != expectedTypes.size()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Invalid number of channels; got %s expected %s; context=%s; blocks=%s; types=%s".formatted(
                            page.getChannelCount(),
                            expectedTypes.size(),
                            debugContextSupplier.get(),
                            blocksDebugInfo(page),
                            expectedTypes));
        }

        // Avoid allocation on the happy path
        List<String> mismatches = null;
        for (int channel = 0; channel < expectedTypes.size(); channel++) {
            Type type = expectedTypes.get(channel);
            Block block = page.getBlock(channel);
            if (!isBlockValidForType(block, type)) {
                if (mismatches == null) {
                    mismatches = new ArrayList<>();
                }
                mismatches.add("Bad block %s for channel %s of type %s".formatted(blockDebugInfo(block), channel, type));
            }
        }
        if (mismatches != null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Bad block types found for context %s; blocks=%s; types=%s; mismatches=%s".formatted(
                            debugContextSupplier.get(),
                            blocksDebugInfo(page),
                            expectedTypes,
                            mismatches));
        }
    }

    private static boolean isBlockValidForType(Block block, Type type)
    {
        ValueBlock underlyingValueBlock = block.getUnderlyingValueBlock();
        return type.getValueBlockType().isInstance(underlyingValueBlock);
    }

    private static String blocksDebugInfo(Page page)
    {
        List<String> debugInfos = new ArrayList<>();
        for (int i = 0; i < page.getChannelCount(); i++) {
            debugInfos.add(blockDebugInfo(page.getBlock(i)));
        }
        return Joiner.on(",").join(debugInfos);
    }

    private static String blockDebugInfo(Block block)
    {
        ValueBlock underlyingValueBlock = block.getUnderlyingValueBlock();
        if (block != underlyingValueBlock) {
            return "%s/%s".formatted(block, underlyingValueBlock);
        }
        else {
            return block.toString();
        }
    }
}
