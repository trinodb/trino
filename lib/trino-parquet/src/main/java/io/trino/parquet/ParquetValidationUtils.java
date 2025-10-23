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
package io.trino.parquet;

import com.google.errorprone.annotations.FormatMethod;
import io.trino.parquet.crypto.ParquetCryptoException;

public final class ParquetValidationUtils
{
    private ParquetValidationUtils() {}

    @FormatMethod
    public static void validateParquet(boolean condition, ParquetDataSourceId dataSourceId, String formatString, Object... args)
            throws ParquetCorruptionException
    {
        if (!condition) {
            throw new ParquetCorruptionException(dataSourceId, formatString, args);
        }
    }

    @FormatMethod
    public static void validateParquetCrypto(boolean condition, ParquetDataSourceId dataSourceId, String formatString, Object... args)
    {
        if (!condition) {
            throw new ParquetCryptoException(dataSourceId, formatString, args);
        }
    }
}
