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

public interface GroupByHashTableEntries
{
    int getEntrySize();

    long getHash(int position);

    byte isNull(int position, int i);

    int capacity();

    int getGroupId(int position);

    void putHash(int position, long hash);

    void putGroupId(int position, int groupId);

    String toString(int position);

    long getEstimatedSize();
}
