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
package io.trino.spi.type;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;

public class TypeNotFoundException
        extends TrinoException
{
    public TypeNotFoundException(TypeSignature type)
    {
        this(type, null);
    }

    public TypeNotFoundException(TypeSignature type, Throwable cause)
    {
        this(type.toString(), cause);
    }

    public TypeNotFoundException(String type, Throwable cause)
    {
        super(TYPE_NOT_FOUND, "Unknown type: " + type, cause);
    }
}
