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

import java.io.IOException;

import static java.lang.String.format;

public class ParquetCorruptionException
        extends IOException
{
    public ParquetCorruptionException(ParquetDataSourceId dataSourceId, String message)
    {
        this(dataSourceId, "%s", message);
    }

    @FormatMethod
    public ParquetCorruptionException(Throwable cause, ParquetDataSourceId dataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(dataSourceId, messageFormat, args), cause);
    }

    @FormatMethod
    public ParquetCorruptionException(ParquetDataSourceId dataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(dataSourceId, messageFormat, args));
    }

    private static String formatMessage(ParquetDataSourceId dataSourceId, String messageFormat, Object[] args)
    {
        return "Malformed Parquet file. " + format(messageFormat, args) + " [" + dataSourceId + "]";
    }
}
