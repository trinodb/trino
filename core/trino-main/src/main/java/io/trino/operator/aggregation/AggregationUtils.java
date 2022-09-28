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
package io.trino.operator.aggregation;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.sql.gen.CompilerOperations;

import javax.annotation.Nullable;

public final class AggregationUtils
{
    private AggregationUtils() {}

    // used by aggregation compiler
    @UsedByGeneratedCode
    @SuppressWarnings("UnusedDeclaration")
    public static Block extractMaskBlock(int maskChannel, Page page)
    {
        if (maskChannel < 0) {
            return null;
        }
        Block maskBlock = page.getBlock(maskChannel);
        if (page.getPositionCount() > 0 && maskBlock instanceof RunLengthEncodedBlock && CompilerOperations.testMask(maskBlock, 0)) {
            return null; // filter out RLE true blocks to bypass unnecessary mask checks
        }
        return maskBlock;
    }

    @UsedByGeneratedCode
    @SuppressWarnings("UnusedDeclaration")
    public static boolean maskGuaranteedToFilterAllRows(int positions, @Nullable Block maskBlock)
    {
        if (maskBlock == null || positions == 0) {
            return false;
        }
        return (maskBlock instanceof RunLengthEncodedBlock && !CompilerOperations.testMask(maskBlock, 0));
    }
}
