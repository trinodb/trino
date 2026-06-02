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
package io.trino.operator;

import io.trino.spi.TrinoException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

record SpatialIndexSridState(int firstSrid, int secondSrid)
{
    static final SpatialIndexSridState EMPTY = new SpatialIndexSridState(0, 0);

    SpatialIndexSridState
    {
        checkArgument(firstSrid != 0 || secondSrid == 0, "second SRID is present without first SRID");
        checkArgument(firstSrid == 0 || firstSrid != secondSrid, "SRIDs are equal: %s", firstSrid);
    }

    SpatialIndexSridState add(int srid)
    {
        if (srid == 0 || srid == firstSrid || srid == secondSrid) {
            return this;
        }
        if (firstSrid == 0) {
            return new SpatialIndexSridState(srid, 0);
        }
        if (secondSrid == 0) {
            return new SpatialIndexSridState(firstSrid, srid);
        }
        return this;
    }

    void validateProbe(int probeSrid)
    {
        if (probeSrid == 0 || firstSrid == 0) {
            return;
        }

        int mismatchedSrid = firstSrid != probeSrid ? firstSrid : secondSrid;
        if (mismatchedSrid != 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SRID mismatch: %s vs %s".formatted(mismatchedSrid, probeSrid));
        }
    }
}
