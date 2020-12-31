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
package io.prestosql.spi.type;

import io.prestosql.spi.PrestoException;

import static io.prestosql.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class TypeNotFoundException
        extends PrestoException
{
    private final TypeSignature type;

    public TypeNotFoundException(TypeSignature type)
    {
        this(type, null);
    }

    public TypeNotFoundException(TypeSignature type, Throwable cause)
    {
        super(TYPE_NOT_FOUND, "Unknown type: " + type, cause);
        this.type = requireNonNull(type, "type is null");
    }

    public TypeSignature getType()
    {
        return type;
    }
}
