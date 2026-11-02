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
package io.trino.sql.gen;

import java.lang.invoke.MethodType;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class Binding
{
    public enum Kind
    {
        /**
         * A method handle stored in the class data of a hidden class, loaded as a dynamic
         * constant and invoked exactly.
         */
        HANDLE,
        /**
         * A constant value stored in the class data of a hidden class, loaded directly
         * as a dynamic constant.
         */
        CONSTANT,
    }

    private final long bindingId;
    private final MethodType type;
    private final Kind kind;

    public Binding(long bindingId, MethodType type, Kind kind)
    {
        this.bindingId = bindingId;
        this.type = type;
        this.kind = kind;
    }

    public long getBindingId()
    {
        return bindingId;
    }

    public MethodType getType()
    {
        return type;
    }

    public Kind getKind()
    {
        return kind;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bindingId", bindingId)
                .add("type", type)
                .add("kind", kind)
                .toString();
    }
}
