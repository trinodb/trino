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
package io.prestosql.operator;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.type.BlockTypeOperators.BlockPositionComparison;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class SimpleChannelComparator
        implements PagesIndexComparator
{
    private final int leftChannel;
    private final int rightChannel;
    private final BlockPositionComparison comparator;

    public SimpleChannelComparator(int leftChannel, int rightChannel, BlockPositionComparison comparator)
    {
        this.leftChannel = leftChannel;
        this.rightChannel = rightChannel;
        this.comparator = requireNonNull(comparator, "comparator is null");
    }

    @Override
    public int compareTo(PagesIndex pagesIndex, int leftPosition, int rightPosition)
    {
        long leftPageAddress = pagesIndex.getValueAddresses().getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = pagesIndex.getValueAddresses().getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        try {
            Block leftBlock = pagesIndex.getChannel(leftChannel).get(leftBlockIndex);
            Block rightBlock = pagesIndex.getChannel(rightChannel).get(rightBlockIndex);
            return (int) comparator.compare(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }
}
