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
package io.trino.parquet.crypto;

import com.google.errorprone.annotations.FormatMethod;
import io.trino.parquet.ParquetDataSourceId;

import java.util.Optional;

import static java.lang.String.format;

public class ParquetCryptoException
        extends RuntimeException
{
    @FormatMethod
    public ParquetCryptoException(String messageFormat, Object... args)
    {
        super(formatMessage(Optional.empty(), messageFormat, args));
    }

    @FormatMethod
    public ParquetCryptoException(Throwable cause, String messageFormat, Object... args)
    {
        super(formatMessage(Optional.empty(), messageFormat, args), cause);
    }

    @FormatMethod
    public ParquetCryptoException(ParquetDataSourceId dataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(Optional.of(dataSourceId), messageFormat, args));
    }

    private static String formatMessage(Optional<ParquetDataSourceId> dataSourceId, String messageFormat, Object[] args)
    {
        if (dataSourceId.isEmpty()) {
            return "Parquet cryptographic error. " + format(messageFormat, args);
        }

        return "Parquet cryptographic error. " + format(messageFormat, args) + " [" + dataSourceId + "]";
    }
}
