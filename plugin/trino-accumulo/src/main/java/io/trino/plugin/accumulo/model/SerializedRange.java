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
package io.trino.plugin.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.accumulo.core.data.Range;
import org.openjdk.jol.info.ClassLayout;

import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SerializedRange
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedRange.class).instanceSize();

    private final byte[] bytes;

    public static SerializedRange serialize(Range range)
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        try {
            range.write(out);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SerializedRange(out.toByteArray());
    }

    @JsonCreator
    public SerializedRange(@JsonProperty("data") byte[] bytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    @JsonProperty
    public byte[] getBytes()
    {
        return bytes;
    }

    public Range deserialize()
    {
        DataInput in = ByteStreams.newDataInput(bytes);
        Range range = new Range();
        try {
            range.readFields(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return range;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(bytes);
    }
}
