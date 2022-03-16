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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;

interface BlockTypeAwarePositionsAppender
        extends PositionsAppender
{
    /**
     * Appends value from the {@code rleBlock} to this appender {@link RunLengthEncodedBlock#getPositionCount()} times.
     * The result is the same as with using {@link PositionsAppender#append(IntArrayList, Block)} with
     * positions list [0...{@link RunLengthEncodedBlock#getPositionCount()} -1]
     * but with possible performance optimizations for {@link RunLengthEncodedBlock}.
     */
    void appendRle(RunLengthEncodedBlock rleBlock);

    /**
     * Appends given {@code positions} from the {@code source} to this appender.
     * The result is the same as with using {@link PositionsAppender#append(IntArrayList, Block)}
     * but with possible performance optimizations for {@link DictionaryBlock}.
     */
    void appendDictionary(IntArrayList positions, DictionaryBlock source);
}
