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
import io.trino.json.JsonItemEncoding.TypeTag;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.airlift.slice.Slices.utf8Slice;

/// SQL/JSON value: the in-memory representation of a JSON document or sub-document.
///
/// Implementations come in two families:
///
/// * **Byte-form** ([EncodedJson]) — backed by the typed-item encoding in a
///   [Slice], the canonical wire representation for block storage and exchange.
///
/// * **Tree-form** ([JsonObject], [JsonArray], [TypedValue], [JsonNullValue],
///   [JsonErrorValue]) — Java object graphs whose byte encoding is materialized
///   lazily on first [#encoding] call.
///
/// `equals` and `hashCode` implement the SQL grouping semantics — multiset object
/// members, cross-type numeric equality, and character comparison by stored text — defined in
/// [JsonItemSemantics], regardless of which backing the operands carry. A byte
/// comparison of two canonical encodings is only a fast path inside that semantic
/// equality (two byte-identical encodings are equal), not the contract itself.
public sealed interface Json
        permits EncodedJson,
                JsonArray,
                JsonErrorValue,
                JsonNullValue,
                JsonObject,
                TypedValue
{
    enum Kind
    {
        NULL,
        ARRAY,
        OBJECT,
        SCALAR,
        ERROR,
    }

    /// Wraps a versioned, self-contained encoded payload. `slice[0]` must be the
    /// [JsonItemEncoding#VERSION] byte; the root item starts at offset 1.
    static Json of(Slice slice)
    {
        return EncodedJson.of(slice);
    }

    // --- discriminators -----------------------------------------------------------

    Kind kind();

    default boolean isNull()
    {
        return kind() == Kind.NULL;
    }

    default boolean isArray()
    {
        return kind() == Kind.ARRAY;
    }

    default boolean isObject()
    {
        return kind() == Kind.OBJECT;
    }

    default boolean isScalar()
    {
        return kind() == Kind.SCALAR;
    }

    default boolean isError()
    {
        return kind() == Kind.ERROR;
    }

    // --- ARRAY accessors. Default throws if not an ARRAY; ARRAY impls override. ---

    default int arraySize()
    {
        throw new IllegalStateException("Not an ARRAY");
    }

    default Json arrayElement(int index)
    {
        throw new IllegalStateException("Not an ARRAY");
    }

    /// Returns a sequential iterator over array elements in input order. A complete traversal
    /// takes linear time for both plain and indexed encodings, without collecting the elements.
    default Iterator<Json> arrayIterator()
    {
        throw new IllegalStateException("Not an ARRAY");
    }

    default void forEachArrayElement(Consumer<Json> consumer)
    {
        throw new IllegalStateException("Not an ARRAY");
    }

    // --- OBJECT accessors. Default throws if not an OBJECT; OBJECT impls override. ---

    default int objectSize()
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    /// Returns member values in input order, including duplicate members, without decoding keys.
    default Iterator<Json> objectValueIterator()
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    default Optional<Json> objectMember(String key)
    {
        return objectMember(utf8Slice(key));
    }

    default Optional<Json> objectMember(Slice keyBytes)
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    default void objectMembers(String key, Consumer<Json> consumer)
    {
        objectMembers(utf8Slice(key), consumer);
    }

    default void objectMembers(Slice keyBytes, Consumer<Json> consumer)
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    default void forEachObjectMember(BiConsumer<String, Json> consumer)
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    /// Slice-keyed variant of [#forEachObjectMember] — avoids a per-call UTF-8 decode for
    /// hot paths (equality, hash, recursive uniqueness check) that compare keys by bytes.
    default void forEachObjectMemberBytes(BiConsumer<Slice, Json> consumer)
    {
        throw new IllegalStateException("Not an OBJECT");
    }

    // --- SCALAR accessors. Default throws if not a SCALAR; SCALAR impls override. ---

    default TypeTag scalarType()
    {
        throw new IllegalStateException("Not a TYPED_VALUE scalar");
    }

    default TypedValue materializeScalar()
    {
        throw new IllegalStateException("Not a TYPED_VALUE scalar");
    }

    // --- byte access. Always returns a fully-encoded form. -------------------------

    /// Returns a fresh, self-contained encoded slice (with the VERSION byte
    /// prepended). For byte-backed implementations this is a copy of the existing
    /// view; for tree forms it materializes the encoding lazily.
    Slice encoding();

    /// Returns the underlying backing slice. May be larger than this view (when the
    /// view points at an inner item of a larger document). For tree
    /// forms this triggers materialization. Use [#encoding] for a self-contained
    /// copy.
    Slice backingSlice();

    /// Byte offset of this item within [#backingSlice].
    int viewOffset();

    /// Byte offset just past this item within [#backingSlice].
    int viewEnd();
}
