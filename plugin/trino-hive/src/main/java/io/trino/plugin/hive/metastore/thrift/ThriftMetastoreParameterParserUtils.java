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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

final class ThriftMetastoreParameterParserUtils
{
    private ThriftMetastoreParameterParserUtils() {}

    static Optional<Boolean> toBoolean(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return Optional.empty();
        }
        Boolean value = Boolean.parseBoolean(parameterValue);
        return Optional.of(value);
    }

    static OptionalLong toLong(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return OptionalLong.empty();
        }
        Long longValue = Longs.tryParse(parameterValue);
        if (longValue == null || longValue < 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(longValue);
    }

    static OptionalDouble toDouble(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return OptionalDouble.empty();
        }
        Double doubleValue = Doubles.tryParse(parameterValue);
        if (doubleValue == null || doubleValue < 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(doubleValue);
    }

    static Optional<BigDecimal> toDecimal(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return Optional.empty();
        }
        try {
            BigDecimal decimal = new BigDecimal(parameterValue);
            if (decimal.compareTo(BigDecimal.ZERO) < 0) {
                return Optional.empty();
            }
            return Optional.of(decimal);
        }
        catch (NumberFormatException exception) {
            return Optional.empty();
        }
    }

    static Optional<LocalDate> toDate(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return Optional.empty();
        }
        try {
            LocalDate date = LocalDate.parse(parameterValue);
            return Optional.of(date);
        }
        catch (DateTimeException exception) {
            return Optional.empty();
        }
    }
}
