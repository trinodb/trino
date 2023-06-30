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

import io.trino.spi.type.ArrayType;

public interface ArrayValueBuilder<E extends Throwable>
{
    static <E extends Throwable> Block buildArrayValue(ArrayType arrayType, int entryCount, ArrayValueBuilder<E> builder)
            throws E
    {
        return new BufferedArrayValueBuilder(arrayType, 1)
                .build(entryCount, builder);
    }

    void build(BlockBuilder elementBuilder)
            throws E;
}
