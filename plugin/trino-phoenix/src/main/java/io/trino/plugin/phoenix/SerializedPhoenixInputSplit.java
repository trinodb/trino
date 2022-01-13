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
package io.trino.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SerializedPhoenixInputSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPhoenixInputSplit.class).instanceSize();

    private final byte[] bytes;

    public static SerializedPhoenixInputSplit serialize(PhoenixInputSplit split)
    {
        return new SerializedPhoenixInputSplit(WritableUtils.toByteArray(split));
    }

    @JsonCreator
    public SerializedPhoenixInputSplit(@JsonProperty("bytes") byte[] bytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    @JsonProperty
    public byte[] getBytes()
    {
        return bytes;
    }

    public PhoenixInputSplit deserialize()
    {
        PhoenixInputSplit split = new PhoenixInputSplit();
        try {
            split.readFields(ByteStreams.newDataInput(bytes));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return split;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(bytes);
    }
}
