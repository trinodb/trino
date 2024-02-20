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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcDataSourceUtils
{
    @Test
    public void testMergeSingle()
    {
        List<DiskRange> diskRanges = mergeAdjacentDiskRanges(
                ImmutableList.of(new DiskRange(100, 100)),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0));
        assertThat(diskRanges).isEqualTo(ImmutableList.of(new DiskRange(100, 100)));
    }

    @Test
    public void testMergeAdjacent()
    {
        List<DiskRange> diskRanges = mergeAdjacentDiskRanges(
                ImmutableList.of(new DiskRange(100, 100), new DiskRange(200, 100), new DiskRange(300, 100)),
                DataSize.ofBytes(0),
                DataSize.of(1, GIGABYTE));
        assertThat(diskRanges).isEqualTo(ImmutableList.of(new DiskRange(100, 300)));
    }

    @Test
    public void testMergeGap()
    {
        List<DiskRange> consistent10ByteGap = ImmutableList.of(new DiskRange(100, 90), new DiskRange(200, 90), new DiskRange(300, 90));
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(0), DataSize.of(1, GIGABYTE))).isEqualTo(consistent10ByteGap);
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(9), DataSize.of(1, GIGABYTE))).isEqualTo(consistent10ByteGap);
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 290)));
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(100), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 290)));

        List<DiskRange> middle10ByteGap = ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 90), new DiskRange(300, 80), new DiskRange(400, 90));
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(0), DataSize.of(1, GIGABYTE))).isEqualTo(middle10ByteGap);
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(9), DataSize.of(1, GIGABYTE))).isEqualTo(middle10ByteGap);
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(10), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 180), new DiskRange(400, 90)));
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(100), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 390)));
    }

    @Test
    public void testMergeMaxSize()
    {
        List<DiskRange> consistent10ByteGap = ImmutableList.of(new DiskRange(100, 90), new DiskRange(200, 90), new DiskRange(300, 90));
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.ofBytes(0))).isEqualTo(consistent10ByteGap);
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.ofBytes(100))).isEqualTo(consistent10ByteGap);
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.ofBytes(190))).isEqualTo(ImmutableList.of(new DiskRange(100, 190), new DiskRange(300, 90)));
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.ofBytes(200))).isEqualTo(ImmutableList.of(new DiskRange(100, 190), new DiskRange(300, 90)));
        assertThat(mergeAdjacentDiskRanges(consistent10ByteGap, DataSize.ofBytes(10), DataSize.ofBytes(290))).isEqualTo(ImmutableList.of(new DiskRange(100, 290)));

        List<DiskRange> middle10ByteGap = ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 90), new DiskRange(300, 80), new DiskRange(400, 90));
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(0), DataSize.of(1, GIGABYTE))).isEqualTo(middle10ByteGap);
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(9), DataSize.of(1, GIGABYTE))).isEqualTo(middle10ByteGap);
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(10), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 180), new DiskRange(400, 90)));
        assertThat(mergeAdjacentDiskRanges(middle10ByteGap, DataSize.ofBytes(100), DataSize.of(1, GIGABYTE))).isEqualTo(ImmutableList.of(new DiskRange(100, 390)));
    }
}
