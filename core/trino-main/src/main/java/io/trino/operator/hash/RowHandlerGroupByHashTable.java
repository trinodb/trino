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

import io.trino.operator.HashGenerator;
import io.trino.spi.Page;

import static java.util.Objects.requireNonNull;

public class RowHandlerGroupByHashTable
        implements GroupByHashTable
{
    private final HashGenerator hashGenerator;
    private final HashTableRowHandler rowHandler;

    public RowHandlerGroupByHashTable(HashGenerator hashGenerator, HashTableRowHandler rowHandler)
    {
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.rowHandler = requireNonNull(rowHandler, "rowHandler is null");
    }

    public int putIfAbsent(HashTableData data, int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        int hashPosition = data.getHashPosition(rawHash);
        // look for an empty slot or a slot containing this key
        while (true) {
            int current = data.getGroupId(hashPosition);

            if (current == -1) {
                // empty slot found
                return addNewGroupId(data, position, page, rawHash, hashPosition);
            }

            if (rowHandler.keyEquals(data.entries(), data.entriesPosition(hashPosition, current), page, position, rawHash)) {
                return current;
            }

            hashPosition = data.nextPosition(hashPosition);
            data.markCollision();
        }
    }

    private int addNewGroupId(HashTableData data, int position, Page page, long rawHash, int hashPosition)
    {
        int groupId = data.addNewGroupId(hashPosition);
        rowHandler.putEntry(data.entries(), data.entriesPosition(hashPosition, groupId), groupId, page, position, rawHash);

        return groupId;
    }

    @Override
    public boolean contains(HashTableData data, int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return contains(data, position, page, rawHash);
    }

    @Override
    public boolean contains(HashTableData data, int position, Page page, long rawHash)
    {
        int hashPosition = data.getHashPosition(rawHash);
        // look for an empty slot or a slot containing this key
        int current;
        while ((current = data.getGroupId(hashPosition)) != -1) {
            if (rowHandler.keyEquals(data.entries(), data.entriesPosition(hashPosition, current), page, position, rawHash)) {
                // found an existing slot for this key
                return true;
            }
            hashPosition = data.nextPosition(hashPosition);
        }

        return false;
    }
}
