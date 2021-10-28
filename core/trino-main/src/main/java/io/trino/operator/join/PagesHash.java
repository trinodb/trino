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
package io.trino.operator.join;

import io.airlift.units.DataSize;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int mask;
    private final int[] key;
    private final long size;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private final long hashCollisions;
    private final double expectedHashCollisions;

    public PagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        positionToHashes = new byte[addresses.size()];

        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition);
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                long hash = positionToFullHashes[position];
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(positionToHashes);
        hashCollisions = hashCollisionsLocal;
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);

        return getAddressIndex(rightPosition, hashChannelsPage, (byte) rawHash, pos);
    }

    public void getAddressIndex(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes, long[] result)
    {
        getAddressIndex2(rightPositions, hashChannelsPage, rawHashes, result);
    }

    public void getAddressIndex3(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes, long[] addressIndexes)
    {
        int[] hashPositions = new int[rightPositions.size()];
        getHashPositions(rightPositions, rawHashes, hashPositions);
        getAddressIndexes(rightPositions, hashPositions, addressIndexes);
        int[] remainingRightPositionList = new int[rightPositions.size()];
        SelectedPositions remainingRightPositions = getNonEmptyPositions(rightPositions, hashPositions, addressIndexes, remainingRightPositionList);
        int hashMismatchedPosCount = splitHashMismatchedPositions(remainingRightPositions, hashPositions, rawHashes, addressIndexes, remainingRightPositionList);
        remainingRightPositions = getMismatchedPositions(hashMismatchedPosCount, remainingRightPositions, hashChannelsPage, hashPositions, addressIndexes, remainingRightPositionList);
        while (remainingRightPositions.size() > 0) {
            incrementHashPositions(remainingRightPositions, hashPositions);
            getAddressIndexes(remainingRightPositions, hashPositions, addressIndexes);
            remainingRightPositions = getNonEmptyPositions(remainingRightPositions, hashPositions, addressIndexes, remainingRightPositionList);
            hashMismatchedPosCount = splitHashMismatchedPositions(remainingRightPositions, hashPositions, rawHashes, addressIndexes, remainingRightPositionList);
            remainingRightPositions = getMismatchedPositions(hashMismatchedPosCount, remainingRightPositions, hashChannelsPage, hashPositions, addressIndexes, remainingRightPositionList);
        }
    }

    private SelectedPositions getNonEmptyPositions(SelectedPositions rightPositions, int[] hashPositions, long[] addressIndexes, int[] remainingRightPositions)
    {
        int remainingPositionCount = 0;
        if (rightPositions.isList()) {
            int[] positionList = rightPositions.getPositions();
            for (int i = 0; i < rightPositions.size(); i++) {
                int position = positionList[i];
                boolean nonEmpty = addressIndexes[position] != -1;
                remainingRightPositions[remainingPositionCount] = position;
                hashPositions[remainingPositionCount] = hashPositions[i];
                remainingPositionCount += nonEmpty ? 1 : 0;
            }
        }
        else {
            for (int position = 0; position < rightPositions.size(); position++) {
                boolean nonEmpty = addressIndexes[position] != -1;
                remainingRightPositions[remainingPositionCount] = position;
                hashPositions[remainingPositionCount] = hashPositions[position];
                remainingPositionCount += nonEmpty ? 1 : 0;
            }
        }
        return getSelectedPositions(rightPositions, remainingRightPositions, remainingPositionCount);
    }

    private int splitHashMismatchedPositions(SelectedPositions rightPositions, int[] hashPositions, long[] rawHashes, long[] addressIndexes, int[] remainingRightPositions)
    {
        // remainingRightPositions array has corresponding right positions
        int hashMismatchedPosCount = 0;
        if (rightPositions.isList()) {
            int[] positionList = rightPositions.getPositions();
            for (int i = 0; i < rightPositions.size(); i++) {
                int rightPosition = positionList[i];
                boolean noMatch = positionToHashes[(int) addressIndexes[rightPosition]] != (byte) rawHashes[rightPosition];
                // swap
                int oldPosition = remainingRightPositions[hashMismatchedPosCount];
                int oldHashPosition = hashPositions[hashMismatchedPosCount];
                remainingRightPositions[hashMismatchedPosCount] = rightPosition;
                hashPositions[hashMismatchedPosCount] = hashPositions[i];
                remainingRightPositions[i] = oldPosition;
                hashPositions[i] = oldHashPosition;
                hashMismatchedPosCount += noMatch ? 1 : 0;
            }
        }
        else {
            for (int rightPosition = 0; rightPosition < rightPositions.size(); rightPosition++) {
                boolean noMatch = positionToHashes[(int) addressIndexes[rightPosition]] != (byte) rawHashes[rightPosition];
                // swap
                int oldPosition = remainingRightPositions[hashMismatchedPosCount];
                int oldHashPosition = hashPositions[hashMismatchedPosCount];
                remainingRightPositions[hashMismatchedPosCount] = rightPosition;
                hashPositions[hashMismatchedPosCount] = hashPositions[rightPosition];
                remainingRightPositions[rightPosition] = oldPosition;
                hashPositions[rightPosition] = oldHashPosition;
                hashMismatchedPosCount += noMatch ? 1 : 0;
            }
        }
        return hashMismatchedPosCount;
    }

    private SelectedPositions getMismatchedPositions(int hashMismatchedPositions, SelectedPositions rightPositions, Page hashChannelsPage, int[] hashPositions, long[] addressIndexes, int[] remainingRightPositions)
    {
        int remainingPositionCount = 0;
        // remainingRightPositions array has corresponding right positions
        if (hashMismatchedPositions != 0 || rightPositions.isList()) {
            for (int i = hashMismatchedPositions; i < rightPositions.size(); i++) {
                int rightPosition = remainingRightPositions[i];
                boolean noMatch = !positionEqualsCurrentRowIgnoreNulls((int) addressIndexes[rightPosition], rightPositions.getOffset() + rightPosition, hashChannelsPage);
                remainingRightPositions[remainingPositionCount + hashMismatchedPositions] = rightPosition;
                hashPositions[remainingPositionCount + hashMismatchedPositions] = hashPositions[i];
                remainingPositionCount += noMatch ? 1 : 0;
            }
        }
        else {
            for (int rightPosition = 0; rightPosition < rightPositions.size(); rightPosition++) {
                boolean noMatch = !positionEqualsCurrentRowIgnoreNulls((int) addressIndexes[rightPosition], rightPositions.getOffset() + rightPosition, hashChannelsPage);
                remainingRightPositions[remainingPositionCount + hashMismatchedPositions] = rightPosition;
                hashPositions[remainingPositionCount + hashMismatchedPositions] = hashPositions[rightPosition];
                remainingPositionCount += noMatch ? 1 : 0;
            }
        }
        return getSelectedPositions(rightPositions, remainingRightPositions, remainingPositionCount + hashMismatchedPositions);
    }

    private void incrementHashPositions(SelectedPositions positions, int[] hashPositions)
    {
        for (int i = 0; i < positions.size(); i++) {
            hashPositions[i] = (hashPositions[i] + 1) & mask;
        }
    }

    private void getAddressIndex1(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes, long[] addressIndexes)
    {
        int[] hashPositions = new int[rightPositions.size()];
        getHashPositions(rightPositions, rawHashes, hashPositions);
        getAddressIndexes(rightPositions, hashPositions, addressIndexes);
        matchPositions(rightPositions, hashChannelsPage, hashPositions, rawHashes, addressIndexes);
        finalMatch(rightPositions, hashChannelsPage, hashPositions, rawHashes, addressIndexes);
    }

    private void matchPositions(SelectedPositions rightPositions, Page hashChannelsPage, int[] hashPositions, long[] rawHashes, long[] addressIndexes)
    {
        if (rightPositions.isList()) {
            int[] rightPositionList = rightPositions.getPositions();
            for (int i = 0; i < rightPositions.size(); i++) {
                int rightPosition = rightPositionList[i];
                matchPosition(rightPositions, hashChannelsPage, hashPositions, rawHashes, addressIndexes, i, rightPosition);
            }
        }
        else {
            for (int rightPosition = 0; rightPosition < rightPositions.size(); rightPosition++) {
                matchPosition(rightPositions, hashChannelsPage, hashPositions, rawHashes, addressIndexes, rightPosition, rightPosition);
            }
        }
    }

    private void matchPosition(SelectedPositions rightPositions, Page hashChannelsPage, int[] hashPositions, long[] rawHashes, long[] addressIndexes, int positionIndex, int rightPosition)
    {
        if (addressIndexes[rightPosition] == -1) {
            hashPositions[positionIndex] = -1;
        }
        else if (positionEqualsCurrentRowIgnoreNulls((int) addressIndexes[rightPosition], (byte) rawHashes[rightPosition], rightPositions.getOffset() + rightPosition, hashChannelsPage)) {
            hashPositions[positionIndex] = -1;
        }
        else {
            hashPositions[positionIndex] = (hashPositions[positionIndex] + 1) & mask;
        }
    }

    private void finalMatch(SelectedPositions rightPositions, Page hashChannelsPage, int[] hashPositions, long[] rawHashes, long[] result)
    {
        if (rightPositions.isList()) {
            int[] rightPositionList = rightPositions.getPositions();
            for (int i = 0; i < rightPositions.size(); i++) {
                int rightPosition = rightPositionList[i];
                if (hashPositions[i] != -1) {
                    result[rightPosition] = getAddressIndex(rightPositions.getOffset() + rightPosition, hashChannelsPage, (byte) rawHashes[rightPosition], hashPositions[i]);
                }
            }
        }
        else {
            for (int rightPosition = 0; rightPosition < rightPositions.size(); rightPosition++) {
                if (hashPositions[rightPosition] != -1) {
                    result[rightPosition] = getAddressIndex(rightPositions.getOffset() + rightPosition, hashChannelsPage, (byte) rawHashes[rightPosition], hashPositions[rightPosition]);
                }
            }
        }
    }

    private void getAddressIndex2(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes, long[] addressIndexes)
    {
        int[] hashPositions = new int[rightPositions.size()];
        getHashPositions(rightPositions, rawHashes, hashPositions);
        getAddressIndexes(rightPositions, hashPositions, addressIndexes);
        int[] remainingRightPositionList = new int[rightPositions.size()];
        SelectedPositions remainingRightPositions = matchPositions(rightPositions, hashChannelsPage, rawHashes, hashPositions, addressIndexes, remainingRightPositionList);
        while (remainingRightPositions.size() > 0) {
            getAddressIndexes(remainingRightPositions, hashPositions, addressIndexes);
            remainingRightPositions = matchPositions(remainingRightPositions, hashChannelsPage, rawHashes, hashPositions, addressIndexes, remainingRightPositionList);
        }
    }

    private SelectedPositions getSelectedPositions(SelectedPositions previousPositions, int[] positionList, int positionCount)
    {
        if (positionCount == previousPositions.size()) {
            return previousPositions;
        }

        return positionsList(positionList, previousPositions.getOffset(), positionCount);
    }

    private SelectedPositions matchPositions(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes, int[] hashPositions, long[] addressIndexes, int[] remainingRightPositions)
    {
        int remainingPositionCount = 0;
        if (rightPositions.isList()) {
            int[] rightPositionList = rightPositions.getPositions();
            for (int i = 0; i < rightPositions.size(); i++) {
                int rightPosition = rightPositionList[i];
                if (addressIndexes[rightPosition] != -1
                        && !positionEqualsCurrentRowIgnoreNulls((int) addressIndexes[rightPosition], (byte) rawHashes[rightPosition], rightPositions.getOffset() + rightPosition, hashChannelsPage)) {
                    hashPositions[remainingPositionCount] = (hashPositions[i] + 1) & mask;
                    remainingRightPositions[remainingPositionCount] = rightPosition;
                    remainingPositionCount++;
                }
            }
        }
        else {
            for (int rightPosition = 0; rightPosition < rightPositions.size(); rightPosition++) {
                if (addressIndexes[rightPosition] != -1
                        && !positionEqualsCurrentRowIgnoreNulls((int) addressIndexes[rightPosition], (byte) rawHashes[rightPosition], rightPositions.getOffset() + rightPosition, hashChannelsPage)) {
                    hashPositions[remainingPositionCount] = (hashPositions[rightPosition] + 1) & mask;
                    remainingRightPositions[remainingPositionCount] = rightPosition;
                    remainingPositionCount++;
                }
            }
        }
        return getSelectedPositions(rightPositions, remainingRightPositions, remainingPositionCount);
    }

    private void getAddressIndexes(SelectedPositions positions, int[] hashPositions, long[] addressIndexes)
    {
        if (positions.isList()) {
            int[] positionList = positions.getPositions();
            for (int i = 0; i < positions.size(); i++) {
                addressIndexes[positionList[i]] = key[hashPositions[i]];
            }
        }
        else {
            for (int position = 0; position < positions.size(); position++) {
                addressIndexes[position] = key[hashPositions[position]];
            }
        }
    }

    private int getAddressIndex(int rightPosition, Page hashChannelsPage, byte rawHash, int pos)
    {
        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private void getHashPositions(SelectedPositions positions, long[] rawHashes, int[] hashPositions)
    {
        if (positions.isList()) {
            int[] positionList = positions.getPositions();
            for (int i = 0; i < positions.size(); i++) {
                hashPositions[i] = getHashPosition(rawHashes[positionList[i]], mask);
            }
        }
        else {
            for (int position = 0; position < positions.size(); position++) {
                hashPositions[position] = getHashPosition(rawHashes[position], mask);
            }
        }
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }
}
