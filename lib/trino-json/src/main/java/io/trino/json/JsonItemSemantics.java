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
import io.airlift.slice.XxHash64;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.AsBigDecimal;
import io.trino.spi.type.TrinoNumber.BigDecimalValue;
import io.trino.spi.type.TrinoNumber.Infinity;
import io.trino.spi.type.TrinoNumber.NotANumber;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

/// Equality of [Json] values, and the hash that goes with it. This is the equality the SQL
/// engine sees: it backs the `EQUAL` / `XX_HASH_64` operators on the JSON type, and therefore
/// `GROUP BY`, `DISTINCT`, joins and hash indexes over JSON columns.
///
/// Two JSON values are equal when they are equivalent:
///
/// - **Scalars** are SQL values, and two scalars are equal when those values are equal. The SQL
///   type a number carries is not part of the JSON number, so `1` (integer), `1` (bigint),
///   `1.0` (double) and `1.00` (decimal) are one value. Character strings compare by their text,
///   which makes a char and a varchar with the same characters equal — a char carries no
///   significant trailing spaces.
/// - **Arrays** are ordered: equal length, and corresponding elements equal.
/// - **Objects** are unordered, and duplicate keys are allowed, so they compare as multisets of
///   (key, value) pairs: `{"a":1,"a":2}` equals `{"a":2,"a":1}` but neither equals
///   `{"a":1,"a":1}`. Keys compare by their characters.
///
/// Hashing mirrors this: array hashing is order-dependent, object hashing sums per-member
/// contributions so member order cannot affect it, and equal numbers hash alike whatever type
/// they arrived as.
///
/// Grouping, not `=`: because this is what `GROUP BY` and `DISTINCT` use, it follows Trino's
/// grouping semantics at the two points where they part company with the `=` predicate — NaN
/// equals NaN, and `-0.0` equals `0.0`.
public final class JsonItemSemantics
{
    private static final long NAN_HASH = 0x7FF8000000000000L;
    private static final long POSITIVE_INFINITY_HASH = 0x7FF0000000000000L;
    private static final long NEGATIVE_INFINITY_HASH = 0xFFF0000000000000L;

    private JsonItemSemantics() {}

    public static boolean equal(Json left, Json right)
    {
        if (left == right) {
            return true;
        }
        // Byte-identical encodings are always equal. Only taken when both operands already hold
        // bytes, so it never forces a tree to materialize just to answer a comparison.
        if (left instanceof EncodedJson leftEncoded && right instanceof EncodedJson rightEncoded && encodingEquals(leftEncoded, rightEncoded)) {
            return true;
        }
        Json.Kind leftKind = left.kind();
        Json.Kind rightKind = right.kind();
        if (leftKind != rightKind) {
            return false;
        }
        return switch (leftKind) {
            case NULL, ERROR -> true;
            case SCALAR -> scalarsEqual(left.materializeScalar(), right.materializeScalar());
            case ARRAY -> arraysEqual(left, right);
            case OBJECT -> objectsEqual(left, right);
        };
    }

    public static long hash(Json json)
    {
        return switch (json.kind()) {
            case NULL, ERROR -> XxHash64.hash(json.encoding());
            case SCALAR -> scalarHash(json.materializeScalar());
            case ARRAY -> arrayHash(json);
            case OBJECT -> objectHash(json);
        };
    }

    /// Equality of two SQL/JSON scalars. A scalar is an SQL value, and two of them are equal
    /// when those values are equal — so the SQL type a number happens to carry does not make it
    /// a different JSON number: `1` (integer), `1` (bigint), `1.0` (double) and `1.00` (decimal)
    /// are one value. Character strings compare by their text, which is why a char and a varchar
    /// holding the same characters are equal (a char carries no significant trailing spaces).
    ///
    /// This backs `equals`/`hashCode`, so it is the equality that `GROUP BY`, `DISTINCT`, joins
    /// and hash indexes see. Those are grouping operations, so it follows Trino's grouping
    /// semantics at the two places they differ from `=`: NaN equals NaN, and `-0.0` equals `0.0`.
    static boolean scalarsEqual(TypedValue left, TypedValue right)
    {
        AsBigDecimal leftNumber = numberOf(left);
        AsBigDecimal rightNumber = numberOf(right);
        if (leftNumber != null || rightNumber != null) {
            return leftNumber != null && rightNumber != null && numbersEqual(leftNumber, rightNumber);
        }

        Slice leftText = textOf(left);
        Slice rightText = textOf(right);
        if (leftText != null || rightText != null) {
            return leftText != null && rightText != null && leftText.equals(rightText);
        }

        // Booleans and datetimes: equal only to the same SQL type carrying the same value.
        return left.type().equals(right.type()) && Objects.equals(left.value(), right.value());
    }

    static long scalarHash(TypedValue scalar)
    {
        AsBigDecimal number = numberOf(scalar);
        if (number != null) {
            return numberHash(number);
        }
        Slice text = textOf(scalar);
        if (text != null) {
            return XxHash64.hash(text);
        }
        return Objects.hash(scalar.type(), scalar.value());
    }

    /// The number a numeric scalar denotes, independent of the SQL type carrying it. `null` for
    /// a scalar that is not a number.
    private static AsBigDecimal numberOf(TypedValue scalar)
    {
        return switch (scalar.type()) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> new BigDecimalValue(BigDecimal.valueOf(scalar.getLongValue()));
            case DoubleType _ -> numberOf(scalar.getDoubleValue());
            case RealType _ -> numberOf(intBitsToFloat(toIntExact(scalar.getLongValue())));
            case DecimalType decimalType -> new BigDecimalValue(decimalOf(decimalType, scalar.value()));
            case NumberType _ -> ((TrinoNumber) scalar.value()).toBigDecimal();
            default -> null;
        };
    }

    private static AsBigDecimal numberOf(double value)
    {
        if (Double.isNaN(value)) {
            return new NotANumber();
        }
        if (Double.isInfinite(value)) {
            return new Infinity(value < 0);
        }
        // BigDecimal has no negative zero, so -0.0 and 0.0 land on the same value.
        return new BigDecimalValue(BigDecimal.valueOf(value));
    }

    private static BigDecimal decimalOf(DecimalType type, Object value)
    {
        BigInteger unscaled = type.isShort()
                ? BigInteger.valueOf((long) value)
                : ((Int128) value).toBigInteger();
        return new BigDecimal(unscaled, type.getScale());
    }

    private static boolean numbersEqual(AsBigDecimal left, AsBigDecimal right)
    {
        return switch (left) {
            case NotANumber _ -> right instanceof NotANumber;
            case Infinity(boolean negative) -> right instanceof Infinity(boolean otherNegative) && negative == otherNegative;
            // compareTo, not equals: BigDecimal equality is scale-sensitive, so 1 and 1.0 would differ.
            case BigDecimalValue(BigDecimal value) -> right instanceof BigDecimalValue(BigDecimal other) && value.compareTo(other) == 0;
        };
    }

    private static long numberHash(AsBigDecimal number)
    {
        return switch (number) {
            case NotANumber _ -> NAN_HASH;
            case Infinity(boolean negative) -> negative ? NEGATIVE_INFINITY_HASH : POSITIVE_INFINITY_HASH;
            case BigDecimalValue(BigDecimal value) -> canonical(value).hashCode();
        };
    }

    /// Strips scale so equal numbers hash alike: 1, 1.0 and 1.00 all reduce to 1. Zero is
    /// special-cased because a zero's scale carries no information.
    private static BigDecimal canonical(BigDecimal value)
    {
        return value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
    }

    private static boolean encodingEquals(EncodedJson left, EncodedJson right)
    {
        Slice leftSlice = left.backingSlice();
        int leftOffset = left.viewOffset();
        int leftLength = left.viewEnd() - leftOffset;
        Slice rightSlice = right.backingSlice();
        int rightOffset = right.viewOffset();
        int rightLength = right.viewEnd() - rightOffset;
        return leftLength == rightLength && leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);
    }

    private static Slice textOf(TypedValue scalar)
    {
        // char and varchar are one character-string value; a char carries no significant
        // trailing spaces, so the stored bytes are the text in both cases.
        if (scalar.type() instanceof VarcharType || scalar.type() instanceof CharType) {
            return (Slice) scalar.getObjectValue();
        }
        return null;
    }

    private static boolean arraysEqual(Json left, Json right)
    {
        int size = left.arraySize();
        if (size != right.arraySize()) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (!equal(left.arrayElement(i), right.arrayElement(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean objectsEqual(Json left, Json right)
    {
        if (left.objectSize() != right.objectSize()) {
            return false;
        }
        Map<Slice, List<Json>> leftMembers = collectMembersByKey(left);
        Map<Slice, List<Json>> rightMembers = collectMembersByKey(right);
        if (!leftMembers.keySet().equals(rightMembers.keySet())) {
            return false;
        }
        for (Map.Entry<Slice, List<Json>> entry : leftMembers.entrySet()) {
            if (!multisetEqual(entry.getValue(), rightMembers.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static Map<Slice, List<Json>> collectMembersByKey(Json object)
    {
        // Slice keys avoid the per-call UTF-8 decode that the String variant would force on
        // every equality. For encoded-form objects the bytes are already in the payload;
        // for tree-form, the JsonObjectMember key field is the canonical bytes.
        Map<Slice, List<Json>> members = new HashMap<>();
        object.forEachObjectMemberBytes((name, value) -> members.computeIfAbsent(name, _ -> new ArrayList<>()).add(value));
        return members;
    }

    private static boolean multisetEqual(List<Json> left, List<Json> right)
    {
        if (left.size() != right.size()) {
            return false;
        }
        if (left.size() == 1) {
            return equal(left.getFirst(), right.getFirst());
        }
        // WITHOUT UNIQUE KEYS lets user/connector JSON repeat a key an arbitrary number of
        // times, and this comparison is on the equality path (GROUP BY / DISTINCT / join), so
        // a linear scan per value would be O(n^2). Bucket the right values by their semantic
        // hash and match each left value within its bucket: distinct-hash values never compare,
        // and a match is consumed by swap-remove (O(1)), so this is near-linear in practice.
        Map<Long, List<Json>> rightByHash = new HashMap<>();
        for (Json value : right) {
            rightByHash.computeIfAbsent(hash(value), _ -> new ArrayList<>()).add(value);
        }
        for (Json value : left) {
            List<Json> bucket = rightByHash.get(hash(value));
            if (bucket == null) {
                return false;
            }
            int found = -1;
            for (int i = 0; i < bucket.size(); i++) {
                if (equal(value, bucket.get(i))) {
                    found = i;
                    break;
                }
            }
            if (found < 0) {
                return false;
            }
            bucket.set(found, bucket.getLast());
            bucket.removeLast();
        }
        return true;
    }

    private static long arrayHash(Json array)
    {
        long h = 1;
        int size = array.arraySize();
        for (int i = 0; i < size; i++) {
            h = h * 31 + hash(array.arrayElement(i));
        }
        return h;
    }

    private static long objectHash(Json object)
    {
        // Sum member contributions so the hash does not depend on member iteration order.
        // Each contribution mixes name and value as a single byte sequence — additive
        // schemes like h(name) * c + h(value) collide on name/value swaps such as
        // {"a":1,"b":2} vs {"a":2,"b":1}, which sum to the same total.
        long[] sum = {0};
        object.forEachObjectMemberBytes((name, value) -> sum[0] += memberHash(name, value));
        return sum[0];
    }

    private static long memberHash(Slice name, Json value)
    {
        // Combine name and value into a non-decomposable hash —
        // hash(name) + hash(value) sums the same when (name, value) pairs swap their
        // values among each other. The value side uses the recursive semantics hash so
        // nested object member-order independence cascades through. `XxHash64.hash(seed,
        // value)` hashes the value 8 bytes under the name-derived seed; no per-member
        // Slice allocation on the GROUP BY / DISTINCT / join hot path.
        return XxHash64.hash(XxHash64.hash(name), hash(value));
    }
}
