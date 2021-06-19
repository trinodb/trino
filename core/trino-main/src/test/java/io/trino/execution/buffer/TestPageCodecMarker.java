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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPageCodecMarker
{
    @Test
    public void testCompressionAndEncryptionMarkers()
    {
        byte compressed = COMPRESSED.set(PageCodecMarker.none());
        byte encrypted = ENCRYPTED.set(PageCodecMarker.none());
        byte compressedAndEncrypted = ENCRYPTED.set(COMPRESSED.set(PageCodecMarker.none()));

        // Not set when no markers present
        assertFalse(COMPRESSED.isSet(PageCodecMarker.none()));
        assertFalse(ENCRYPTED.isSet(PageCodecMarker.none()));

        // Setting Markers
        assertTrue(COMPRESSED.isSet(compressed));
        assertTrue(COMPRESSED.isSet(compressedAndEncrypted));
        assertTrue(ENCRYPTED.isSet(encrypted));
        assertTrue(ENCRYPTED.isSet(compressedAndEncrypted));

        // Unsetting Markers
        assertEquals(COMPRESSED.unset(compressed), PageCodecMarker.none());
        assertEquals(ENCRYPTED.unset(encrypted), PageCodecMarker.none());
        assertFalse(COMPRESSED.isSet(COMPRESSED.unset(compressedAndEncrypted)));
        assertFalse(ENCRYPTED.isSet(ENCRYPTED.unset(compressedAndEncrypted)));

        // Summary String
        assertEquals(PageCodecMarker.toSummaryString(PageCodecMarker.none()), "NONE");
        assertEquals(PageCodecMarker.toSummaryString(encrypted), "ENCRYPTED");
        assertEquals(PageCodecMarker.toSummaryString(compressed), "COMPRESSED");
        assertEquals(PageCodecMarker.toSummaryString(compressedAndEncrypted), "COMPRESSED, ENCRYPTED");
    }

    @Test
    public void testIsSet()
    {
        assertEquals((byte) 0, PageCodecMarker.none());

        PageCodecMarker.MarkerSet markerSet = PageCodecMarker.MarkerSet.empty();

        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertFalse(marker.isSet(PageCodecMarker.none()));
            assertFalse(markerSet.contains(marker));

            markerSet.add(marker);
            assertTrue(markerSet.contains(marker));
            assertTrue(marker.isSet(marker.set(PageCodecMarker.none())));

            markerSet.remove(marker);
            assertFalse(markerSet.contains(marker));
            assertFalse(marker.isSet(marker.unset(marker.set(PageCodecMarker.none()))));

            for (PageCodecMarker other : PageCodecMarker.values()) {
                assertEquals(other == marker, PageCodecMarker.MarkerSet.of(marker).contains(other));
                assertEquals(other == marker, other.isSet(marker.set(PageCodecMarker.none())));
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
        assertEquals(PageCodecMarker.toSummaryString(PageCodecMarker.none()), "NONE");
        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertEquals(PageCodecMarker.toSummaryString(marker.set(PageCodecMarker.none())), marker.name());
            assertTrue(PageCodecMarker.MarkerSet.of(marker).toString().contains(marker.name()));
            allMarkers = marker.set(allMarkers);
        }

        PageCodecMarker.MarkerSet allInSet = PageCodecMarker.MarkerSet.fromByteValue(allMarkers);
        String allMarkersSummary = PageCodecMarker.toSummaryString(allMarkers);
        assertTrue(allInSet.toString().contains(allMarkersSummary));

        for (PageCodecMarker marker : PageCodecMarker.values()) {
            assertTrue(allMarkersSummary.contains(marker.name()));
            assertTrue(allInSet.contains(marker));
        }
    }
}
