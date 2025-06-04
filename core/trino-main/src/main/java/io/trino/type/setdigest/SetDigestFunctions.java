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

package io.trino.type.setdigest;

import io.airlift.slice.Slice;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.type.setdigest.SetDigest.exactIntersectionCardinality;

public final class SetDigestFunctions
{
    private SetDigestFunctions() {}

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(StandardTypes.SET_DIGEST) Slice digest)
    {
        return SetDigest.newInstance(digest).cardinality();
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long intersectionCardinality(@SqlType(StandardTypes.SET_DIGEST) Slice slice1, @SqlType(StandardTypes.SET_DIGEST) Slice slice2)
    {
        SetDigest digest1 = SetDigest.newInstance(slice1);
        SetDigest digest2 = SetDigest.newInstance(slice2);

        if (digest1.isExact() && digest2.isExact()) {
            return exactIntersectionCardinality(digest1, digest2);
        }

        long cardinality1 = digest1.cardinality();
        long cardinality2 = digest2.cardinality();
        double jaccard = SetDigest.jaccardIndex(digest1, digest2);
        digest1.mergeWith(digest2);
        long result = Math.round(jaccard * digest1.cardinality());

        // When one of the sets is much smaller than the other and approaches being a true
        // subset of the other, the computed cardinality may exceed the cardinality estimate
        // of the smaller set. When this happens the cardinality of the smaller set is obviously
        // a better estimate of the one computed with the Jaccard Index.
        return Math.min(result, Math.min(cardinality1, cardinality2));
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double jaccardIndex(@SqlType(StandardTypes.SET_DIGEST) Slice slice1, @SqlType(StandardTypes.SET_DIGEST) Slice slice2)
    {
        SetDigest digest1 = SetDigest.newInstance(slice1);
        SetDigest digest2 = SetDigest.newInstance(slice2);

        return SetDigest.jaccardIndex(digest1, digest2);
    }

    @ScalarFunction
    @SqlType("map(bigint,smallint)")
    public static SqlMap hashCounts(@TypeParameter("map(bigint,smallint)") Type mapType, @SqlType(StandardTypes.SET_DIGEST) Slice slice)
    {
        SetDigest digest = SetDigest.newInstance(slice);
        return buildMapValue(
                ((MapType) mapType),
                digest.getHashCounts().size(),
                (keyBuilder, valueBuilder) -> {
                    digest.getHashCounts().forEach((key, value) -> {
                        BIGINT.writeLong(keyBuilder, key);
                        SMALLINT.writeLong(valueBuilder, value);
                    });
                });
    }
}
