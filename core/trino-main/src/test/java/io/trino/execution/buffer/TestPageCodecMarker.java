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

import org.testng.annotations.Test;

import static io.trino.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.trino.execution.buffer.PageCodecMarker.ENCRYPTED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageCodecMarker
{
    @Test
    public void testCompressionAndEncryptionMarkers()
    {
        byte compressed = COMPRESSED.set(PageCodecMarker.none());
        byte encrypted = ENCRYPTED.set(PageCodecMarker.none());
        byte compressedAndEncrypted = ENCRYPTED.set(COMPRESSED.set(PageCodecMarker.none()));

        // Not set when no markers present
        assertThat(COMPRESSED.isSet(PageCodecMarker.none())).isFalse();
        assertThat(ENCRYPTED.isSet(PageCodecMarker.none())).isFalse();

        // Setting Markers
        assertThat(COMPRESSED.isSet(compressed)).isTrue();
        assertThat(COMPRESSED.isSet(compressedAndEncrypted)).isTrue();
        assertThat(ENCRYPTED.isSet(encrypted)).isTrue();
        assertThat(ENCRYPTED.isSet(compressedAndEncrypted)).isTrue();

        // Unsetting Markers
        assertThat(COMPRESSED.unset(compressed)).isEqualTo(PageCodecMarker.none());
        assertThat(ENCRYPTED.unset(encrypted)).isEqualTo(PageCodecMarker.none());
        assertThat(COMPRESSED.isSet(COMPRESSED.unset(compressedAndEncrypted))).isFalse();
        assertThat(ENCRYPTED.isSet(ENCRYPTED.unset(compressedAndEncrypted))).isFalse();

        // Summary String
        assertThat(PageCodecMarker.toSummaryString(PageCodecMarker.none())).isEqualTo("NONE");
        assertThat(PageCodecMarker.toSummaryString(encrypted)).isEqualTo("ENCRYPTED");
        assertThat(PageCodecMarker.toSummaryString(compressed)).isEqualTo("COMPRESSED");
        assertThat(PageCodecMarker.toSummaryString(compressedAndEncrypted)).isEqualTo("COMPRESSED, ENCRYPTED");
    }

    @Test
    public void testIsSet()
    {
        assertThat((byte) 0).isEqualTo(PageCodecMarker.none());

        PageCodecMarker.MarkerSet markerSet = PageCodecMarker.MarkerSet.empty();

        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertThat(marker.isSet(PageCodecMarker.none())).isFalse();
            assertThat(markerSet.contains(marker)).isFalse();

            markerSet.add(marker);
            assertThat(markerSet.contains(marker)).isTrue();
            assertThat(marker.isSet(marker.set(PageCodecMarker.none()))).isTrue();

            markerSet.remove(marker);
            assertThat(markerSet.contains(marker)).isFalse();
            assertThat(marker.isSet(marker.unset(marker.set(PageCodecMarker.none())))).isFalse();

            for (PageCodecMarker other : PageCodecMarker.values()) {
                assertThat(other == marker).isEqualTo(PageCodecMarker.MarkerSet.of(marker).contains(other));
                assertThat(other == marker).isEqualTo(other.isSet(marker.set(PageCodecMarker.none())));
            }
        }
    }

    @Test
    public void testMarkerSetEquivalenceToByteMask()
    {
        byte markers = PageCodecMarker.none();
        PageCodecMarker.MarkerSet markerSet = PageCodecMarker.MarkerSet.empty();

        for (PageCodecMarker m : PageCodecMarker.values()) {
            markers = m.set(markers);
            markerSet.add(m);
        }
    }

    @Test
    public void testSummaryString()
    {
        byte allMarkers = PageCodecMarker.none();
        assertThat(PageCodecMarker.toSummaryString(PageCodecMarker.none())).isEqualTo("NONE");
        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertThat(PageCodecMarker.toSummaryString(marker.set(PageCodecMarker.none()))).isEqualTo(marker.name());
            assertThat(PageCodecMarker.MarkerSet.of(marker).toString().contains(marker.name())).isTrue();
            allMarkers = marker.set(allMarkers);
        }

        PageCodecMarker.MarkerSet allInSet = PageCodecMarker.MarkerSet.fromByteValue(allMarkers);
        String allMarkersSummary = PageCodecMarker.toSummaryString(allMarkers);
        assertThat(allInSet.toString().contains(allMarkersSummary)).isTrue();

        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertThat(allMarkersSummary.contains(marker.name())).isTrue();
            assertThat(allInSet.contains(marker)).isTrue();
        }
    }
}
