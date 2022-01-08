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
package io.trino.execution.buffer;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Encodes boolean properties for serialized page by using a bitmasking strategy, allowing
 * up to 8 such properties to be stored in a single byte
 */
enum PageCodecMarker
{
    COMPRESSED(1),
    ENCRYPTED(2);

    private final int mask;

    PageCodecMarker(int bit)
    {
        checkArgument(bit > 0 && bit <= 8, "PageCodecMarker bit must be between 1 and 8. Found: %s", bit);
        this.mask = (1 << (bit - 1));
    }

    public boolean isSet(byte value)
    {
        return (Byte.toUnsignedInt(value) & mask) == mask;
    }

    public byte set(byte value)
    {
        return (byte) (Byte.toUnsignedInt(value) | mask);
    }

    public byte unset(byte value)
    {
        return (byte) (Byte.toUnsignedInt(value) & (~mask));
    }

    /**
     * The byte value of no {@link PageCodecMarker} values set to true
     */
    public static byte none()
    {
        return 0;
    }

    public static String toSummaryString(byte markers)
    {
        if (markers == none()) {
            return "NONE";
        }
        return Arrays.stream(values())
                .filter(marker -> marker.isSet(markers))
                .map(PageCodecMarker::name)
                .collect(Collectors.joining(", "));
    }

    public static final class MarkerSet
    {
        private byte markers;

        private MarkerSet(byte markers)
        {
            this.markers = markers;
        }

        public void add(PageCodecMarker marker)
        {
            markers = marker.set(markers);
        }

        public boolean contains(PageCodecMarker marker)
        {
            return marker.isSet(markers);
        }

        public void remove(PageCodecMarker marker)
        {
            markers = marker.unset(markers);
        }

        public byte byteValue()
        {
            return markers;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("markers", toSummaryString(markers))
                    .toString();
        }

        @Override
        public int hashCode()
        {
            return markers;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o instanceof MarkerSet) {
                return markers == ((MarkerSet) o).markers;
            }
            return false;
        }

        public static MarkerSet of(PageCodecMarker marker)
        {
            return fromByteValue(marker.set(PageCodecMarker.none()));
        }

        public static MarkerSet fromByteValue(byte markers)
        {
            return new MarkerSet(markers);
        }

        public static MarkerSet empty()
        {
            return fromByteValue(PageCodecMarker.none());
        }
    }
}
