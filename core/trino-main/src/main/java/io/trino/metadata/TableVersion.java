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
package io.trino.metadata;

import io.trino.spi.connector.PointerType;
import io.trino.spi.type.Type;

public class TableVersion
{
    private final PointerType pointerType;
    private final Type objectType;
    private final Object pointer;

    public TableVersion(PointerType pointerType, Type objectType, Object pointer)
    {
        this.pointerType = pointerType;
        this.objectType = objectType;
        this.pointer = pointer;
    }

    public PointerType getPointerType()
    {
        return pointerType;
    }

    public Type getObjectType()
    {
        return objectType;
    }

    public Object getPointer()
    {
        return pointer;
    }
}
