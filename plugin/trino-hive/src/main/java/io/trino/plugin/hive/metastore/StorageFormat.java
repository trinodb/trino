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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.TrinoException;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static java.util.Objects.requireNonNull;

@Immutable
public class StorageFormat
{
    public static final StorageFormat VIEW_STORAGE_FORMAT = createNullable(null, null, null);

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    private StorageFormat(String serde, String inputFormat, String outputFormat)
    {
        this.serde = serde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public String getSerde()
    {
        if (serde == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "SerDe is not present in StorageFormat");
        }
        return serde;
    }

    public String getInputFormat()
    {
        if (inputFormat == null) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "InputFormat is not present in StorageFormat");
        }
        return inputFormat;
    }

    public String getOutputFormat()
    {
        if (outputFormat == null) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "OutputFormat is not present in StorageFormat");
        }
        return outputFormat;
    }

    @JsonProperty("serde")
    public String getSerDeNullable()
    {
        return serde;
    }

    @JsonProperty("inputFormat")
    public String getInputFormatNullable()
    {
        return inputFormat;
    }

    @JsonProperty("outputFormat")
    public String getOutputFormatNullable()
    {
        return outputFormat;
    }

    public static StorageFormat fromHiveStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        return new StorageFormat(hiveStorageFormat.getSerde(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat());
    }

    public static StorageFormat create(String serde, String inputFormat, String outputFormat)
    {
        return new StorageFormat(
                requireNonNull(serde, "serde is null"),
                requireNonNull(inputFormat, "inputFormat is null"),
                requireNonNull(outputFormat, "outputFormat is null"));
    }

    @JsonCreator
    public static StorageFormat createNullable(
            @JsonProperty("serde") String serde,
            @JsonProperty("inputFormat") String inputFormat,
            @JsonProperty("outputFormat") String outputFormat)
    {
        return new StorageFormat(serde, inputFormat, outputFormat);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageFormat that = (StorageFormat) o;
        return Objects.equals(serde, that.serde) &&
                Objects.equals(inputFormat, that.inputFormat) &&
                Objects.equals(outputFormat, that.outputFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(serde, inputFormat, outputFormat);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("serde", serde)
                .add("inputFormat", inputFormat)
                .add("outputFormat", outputFormat)
                .toString();
    }
}
