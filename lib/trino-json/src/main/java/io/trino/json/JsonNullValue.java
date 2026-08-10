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
package io.trino.json;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/// JSON null singleton. Carries a pre-built canonical encoded slice so
/// [#encoding] / [#backingSlice] are O(1).
public final class JsonNullValue
        implements Json
{
    // A one-byte item tag on the wire.

    public static final JsonNullValue INSTANCE = new JsonNullValue();

    private static final Slice ENCODED;

    static {
        byte[] bytes = new byte[2];
        bytes[0] = JsonItemEncoding.VERSION;
        bytes[1] = JsonItemEncoding.ItemTag.JSON_NULL.encoded();
        ENCODED = Slices.wrappedBuffer(bytes);
    }

    private JsonNullValue() {}

    @Override
    public Kind kind()
    {
        return Kind.NULL;
    }

    @Override
    public boolean isNull()
    {
        return true;
    }

    @Override
    public Slice encoding()
    {
        return ENCODED;
    }

    @Override
    public Slice backingSlice()
    {
        return ENCODED;
    }

    @Override
    public int viewOffset()
    {
        return 1;
    }

    @Override
    public int viewEnd()
    {
        return ENCODED.length();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Json that)) {
            return false;
        }
        return JsonItemSemantics.equal(this, that);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(JsonItemSemantics.hash(this));
    }

    @Override
    public String toString()
    {
        return "Json[NULL]";
    }
}
