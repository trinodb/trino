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
package io.trino.array;

public interface IntSegmentConsumer
{
    /**
     * Consumes a contiguous run of elements that live within a single backing segment.
     *
     * @param segment the backing array holding the run
     * @param offset the index of the first element within {@code segment}
     * @param length the number of elements in the run, starting at {@code offset}
     */
    void accept(int[] segment, int offset, int length);
}
