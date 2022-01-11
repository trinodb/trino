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

public interface GroupByHashTableEntries
{
    int getEntrySize();

    long getHash(int position);

    boolean keyEquals(int position, GroupByHashTableEntries other, int otherPosition);

    void close();

    FastByteBuffer takeOverflow();

    byte isNull(int position, int i);

    void putEntry(int hashPosition, int groupId, GroupByHashTableEntries key);

    int capacity();

    String toString(int position);

    int getGroupId(int position);

    void copyEntryFrom(GroupByHashTableEntries src, int srcPosition, int toPosition);

    long getEstimatedSize();

    void putHash(int position, long hash);

    void putGroupId(int position, int groupId);

    default boolean isOverflow(int position)
    {
        return getOverflowLength(position) != 0;
    }

    int getOverflowLength(int position);

    GroupByHashTableEntries extend(int newCapacity);

    int maxEntryCount();
}
