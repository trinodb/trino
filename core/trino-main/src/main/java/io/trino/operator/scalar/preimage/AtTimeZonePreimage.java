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
package io.trino.operator.scalar.preimage;

import io.airlift.slice.Slice;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;

public final class AtTimeZonePreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        return isComparisonIdentity(context) ? Optional.of(new PreimageResult(resultDomain, EXACT)) : Optional.empty();
    }

    @Override
    public boolean isComparisonIdentity(Context context)
    {
        if (context.signature().getArgumentTypes().size() != 2 || context.inputArgument() != 0 ||
                !(context.signature().getReturnType() instanceof TimestampWithTimeZoneType) ||
                !context.signature().getArgumentTypes().getFirst().equals(context.signature().getReturnType())) {
            return false;
        }
        Slice zone = (Slice) context.arguments().get(1).orElseThrow().getValue();
        try {
            getTimeZoneKey(zone.toStringUtf8());
            return true;
        }
        catch (TimeZoneNotSupportedException | IllegalArgumentException _) {
            return false;
        }
    }
}
