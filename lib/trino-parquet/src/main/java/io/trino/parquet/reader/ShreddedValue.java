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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.trino.spi.variant.Variant;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The decoded shredded representation of a single Variant value at one row, following the Parquet
 * Variant shredding spec (a node is a {@code value} column plus an optional {@code typed_value}).
 * Reconstruction of the unshredded Variant is performed by {@link VariantShreddingReconstructor}.
 * <p>
 * At every node {@code value} holds the (partial or whole) Variant-encoded value that was not
 * shredded, encoded against the row's original metadata dictionary, or empty when the {@code value}
 * column is null. The typed part is present only when the {@code typed_value} column is non-null at
 * this row.
 */
public sealed interface ShreddedValue
        permits ShreddedValue.ShreddedArray,
                ShreddedValue.ShreddedObject,
                ShreddedValue.ShreddedScalar
{
    Optional<Slice> value();

    /**
     * A scalar shredded leaf. {@code typedValue} is the shredded primitive already decoded into a
     * Variant (for example via {@link Variant#ofLong}), or empty when the {@code typed_value} column
     * is null at this row.
     */
    record ShreddedScalar(Optional<Slice> value, Optional<Variant> typedValue)
            implements ShreddedValue
    {
        public ShreddedScalar
        {
            requireNonNull(value, "value is null");
            requireNonNull(typedValue, "typedValue is null");
        }
    }

    /**
     * An object shredded node. {@code shreddedFields} maps each shredded field name to its child
     * node, or is empty when the {@code typed_value} object group is null at this row (the value is
     * not a shredded object). When both {@code value} and {@code shreddedFields} are present the
     * object is partially shredded and the two are merged.
     */
    record ShreddedObject(Optional<Slice> value, Optional<Map<Slice, ShreddedValue>> shreddedFields)
            implements ShreddedValue
    {
        public ShreddedObject
        {
            requireNonNull(value, "value is null");
            requireNonNull(shreddedFields, "shreddedFields is null");
        }
    }

    /**
     * An array shredded node. {@code typedElements} holds one child node per element, or is empty
     * when the {@code typed_value} list is null at this row. Per the spec every array element is
     * present (arrays cannot contain missing elements).
     */
    record ShreddedArray(Optional<Slice> value, Optional<List<ShreddedValue>> typedElements)
            implements ShreddedValue
    {
        public ShreddedArray
        {
            requireNonNull(value, "value is null");
            requireNonNull(typedElements, "typedElements is null");
        }
    }
}
