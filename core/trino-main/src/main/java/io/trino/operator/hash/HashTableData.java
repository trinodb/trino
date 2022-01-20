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

public interface HashTableData
{
    int getPosition(int groupId);

    int getHashPosition(long rawHash);

    boolean needRehash();

    int getGroupId(int hashPosition);

    int addNewGroupId(int hashPosition);

    GroupByHashTableEntries entries();

    int nextPosition(int hashPosition);

    void markCollision();

    long getEstimatedSize();

    long getHashCollisions();

    int getHashTableSize();

    int getCapacity();

    int maxFill();

    long getRawHash(int groupId);

    HashTableData resize(int newCapacity);

    int entriesPosition(int hashPosition, int groupId);
}
