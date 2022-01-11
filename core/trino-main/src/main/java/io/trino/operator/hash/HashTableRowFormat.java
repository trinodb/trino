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
package io.trino.operator.hash;

import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface HashTableRowFormat
{
    void copyToTable(Page page, int position, GroupByHashTableEntries entries, int entriesPosition);

    void appendValuesTo(GroupByHashTableEntries entries, int position, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash);

    GroupByHashTableEntries allocateRowBuffer(int hashChannelsCount);

    GroupByHashTableEntries allocateHashTableEntries(int hashChannelsCount, int hashCapacity, FastByteBuffer overflow);

    void putEntry(GroupByHashTableEntries entries, int entriesPosition, int groupId, Page page, int position, long rawHash);

    boolean keyEquals(GroupByHashTableEntries entries, int entriesPosition, Page page, int position, long rawHash);
}
