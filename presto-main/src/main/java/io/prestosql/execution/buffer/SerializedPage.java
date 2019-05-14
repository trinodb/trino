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
package io.prestosql.execution.buffer;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.prestosql.execution.buffer.PageCodecMarker.ENCRYPTED;
import static java.util.Objects.requireNonNull;

public class SerializedPage
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPage.class).instanceSize();

    private final Slice slice;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final byte pageCodecMarkers;

    public SerializedPage(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.pageCodecMarkers = requireNonNull(markers, "markers is null").byteValue();
        //  Encrypted pages may include arbitrary overhead from ciphers, sanity checks skipped
        if (!markers.contains(ENCRYPTED)) {
            if (markers.contains(COMPRESSED)) {
                checkArgument(uncompressedSizeInBytes > slice.length(), "compressed size must be smaller than uncompressed size when compressed");
            }
            else {
                checkArgument(uncompressedSizeInBytes == slice.length(), "uncompressed size must be equal to slice length when uncompressed");
            }
        }
    }

    public int getSizeInBytes()
    {
        return slice.length();
    }

    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + slice.getRetainedSize();
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    public boolean isCompressed()
    {
        return COMPRESSED.isSet(pageCodecMarkers);
    }

    public boolean isEncrypted()
    {
        return ENCRYPTED.isSet(pageCodecMarkers);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("pageCodecMarkers", PageCodecMarker.toSummaryString(pageCodecMarkers))
                .add("sizeInBytes", slice.length())
                .add("uncompressedSizeInBytes", uncompressedSizeInBytes)
                .toString();
    }
}
