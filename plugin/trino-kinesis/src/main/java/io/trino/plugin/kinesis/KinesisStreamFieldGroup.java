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
package io.trino.plugin.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.kinesis.KinesisCompressionCodec.UNCOMPRESSED;
import static java.util.Objects.requireNonNull;

public class KinesisStreamFieldGroup
{
    private final String dataFormat;
    private final Optional<KinesisCompressionCodec> compressionCodec;
    private final List<KinesisStreamFieldDescription> fields;

    @JsonCreator
    public KinesisStreamFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("compressionCodec") Optional<KinesisCompressionCodec> compressionCodec,
            @JsonProperty("fields") List<KinesisStreamFieldDescription> fields)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    public KinesisCompressionCodec getCompressionCodec()
    {
        return compressionCodec.orElse(UNCOMPRESSED);
    }

    @JsonProperty
    public List<KinesisStreamFieldDescription> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("compressionCodec", compressionCodec)
                .add("fields", fields)
                .toString();
    }
}
