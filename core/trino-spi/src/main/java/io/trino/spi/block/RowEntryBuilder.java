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
package io.trino.spi.block;

public interface RowEntryBuilder
{
    /**
     * Get the BlockBuilder for a specific field in the row.
     * Only a single value should be appended to the returned BlockBuilder before calling build().
     * The block builder must not be retained or used after build() is called.
     */
    BlockBuilder getFieldBuilder(int fieldId);

    /**
     * Finalize the entry after ALL field values have been appended to the field builders.
     * This method MUST be called exactly once after all field builders have been used.
     */
    void build();
}
