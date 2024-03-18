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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.spi.connector.ConnectorPageSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeDataServiceTest
{
    private ShortBuffer rowsBuffer;
    private RangeFillerService rangeFillerService;

    private QueryParams queryParams;

    @BeforeEach
    public void before()
    {
        ByteBuffer rowsByteBuffer = ByteBuffer.allocate(100);
        rowsBuffer = rowsByteBuffer.asShortBuffer();

        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.ids2RowsBuff(anyLong())).thenReturn(rowsBuffer);

        queryParams = mock(QueryParams.class);
        when(queryParams.getTotalNumRecords()).thenReturn(80);
        when(queryParams.getNumCollectElements()).thenReturn(1);
        when(queryParams.getCollectElementsParamsList()).thenReturn(Collections.emptyList());
        rangeFillerService = new NativeRangeFillerService(bufferAllocator);
    }

    @Test
    public void testFullRange()
    {
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_FULL.ordinal());
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(64);
        rangeFillerService.add(0, 1, storageCollectorArgs, true, rangeData);
        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(1);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(0);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(64);
    }

    @Test
    public void testAllOneShot()
    {
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) 8); // first row is 3 and we add the size

        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);
        rangeFillerService.add(0, 5, storageCollectorArgs, true, rangeData);
        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(1);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(8);
    }

    @Test
    public void testAllTwoShots()
    {
        rowsBuffer.position(0);
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) 8); // first row is 3 and we add the size
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);
        rangeFillerService.add(0, 5, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) 18); // first row is 8 and we add the size
        rangeFillerService.add(0, 10, storageCollectorArgs, true, rangeData);

        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(1);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(18);
    }

    @Test
    public void testValuesOneShot()
    {
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        // 1st range [3-6)
        rowsBuffer.put((short) 3);
        rowsBuffer.put((short) 4);
        rowsBuffer.put((short) 5);
        // 2nd range [30-31)
        rowsBuffer.put((short) 30);
        // 3rd range [49-54)
        rowsBuffer.put((short) 49);
        rowsBuffer.put((short) 50);
        rowsBuffer.put((short) 51);
        rowsBuffer.put((short) 52);
        rowsBuffer.put((short) 53);
        // 4th range [63-64)
        rowsBuffer.put((short) 63);
        rangeFillerService.add(0, 10, storageCollectorArgs, true, rangeData);

        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(4);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(6);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(30);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(31);
        assertThat(ranges.getLowerInclusive(2)).isEqualTo(49);
        assertThat(ranges.getUpperExclusive(2)).isEqualTo(54);
        assertThat(ranges.getLowerInclusive(3)).isEqualTo(63);
        assertThat(ranges.getUpperExclusive(3)).isEqualTo(64);
    }

    @Test
    public void testValuesTwoShots()
    {
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);
        int pos;
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        // 1st range [3-6)
        rowsBuffer.put((short) 3);
        rowsBuffer.put((short) 4);
        rowsBuffer.put((short) 5);
        // 2nd range [30-31)
        rowsBuffer.put((short) 30);
        // 3rd range [49-54)
        rowsBuffer.put((short) 49);
        rowsBuffer.put((short) 50);
        pos = rowsBuffer.position();
        rangeFillerService.add(0, 6, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(pos);
        rowsBuffer.put((short) 51);
        rowsBuffer.put((short) 52);
        rowsBuffer.put((short) 53);
        // 4th range [63-64)
        rowsBuffer.put((short) 63);
        rangeFillerService.add(0, 4, storageCollectorArgs, true, rangeData);

        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(4);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(6);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(30);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(31);
        assertThat(ranges.getLowerInclusive(2)).isEqualTo(49);
        assertThat(ranges.getUpperExclusive(2)).isEqualTo(54);
        assertThat(ranges.getLowerInclusive(3)).isEqualTo(63);
        assertThat(ranges.getUpperExclusive(3)).isEqualTo(64);
    }

    @Test
    public void testAllAndThenValues()
    {
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);
        int pos = 5;
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) (3 + pos)); // first row is 3 and we add the size
        rangeFillerService.add(0, pos, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal() + pos);
        rowsBuffer.put((short) 8);
        rowsBuffer.put((short) 9);
        pos = rowsBuffer.position();
        rangeFillerService.add(0, 2, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(pos);
        rowsBuffer.put((short) 13);
        rowsBuffer.put((short) 14);
        rowsBuffer.put((short) 15);
        rangeFillerService.add(0, 3, storageCollectorArgs, true, rangeData);

        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(2);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(10);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(13);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(16);
    }

    @Test
    public void testValuesAndThenAllAndThenValues()
    {
        RangeData rangeData = new RangeData(0L);
        StorageCollectorArgs storageCollectorArgs = mock(StorageCollectorArgs.class);
        when(storageCollectorArgs.queryParams()).thenReturn(queryParams);
        when(storageCollectorArgs.chunkSize()).thenReturn(1);
        int pos;
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) 25);
        rowsBuffer.put((short) 26);
        rowsBuffer.put((short) 27);
        pos = rowsBuffer.position();
        rangeFillerService.add(0, 3, storageCollectorArgs, true, rangeData);

        short all = 22;
        pos += all;
        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL.ordinal());
        rowsBuffer.position(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal());
        rowsBuffer.put((short) (28 + all)); // first row is 28 and we add the size
        rangeFillerService.add(0, all, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(pos);
        rowsBuffer.put((short) 50);
        pos = rowsBuffer.position();
        rangeFillerService.add(0, 1, storageCollectorArgs, true, rangeData);

        rowsBuffer.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuffer.position(pos);
        rowsBuffer.put((short) 60);
        rangeFillerService.add(0, 1, storageCollectorArgs, true, rangeData);

        ConnectorPageSource.RowRanges ranges = rangeFillerService.reset(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(2);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(25);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(51);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(60);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(61);
    }
}
