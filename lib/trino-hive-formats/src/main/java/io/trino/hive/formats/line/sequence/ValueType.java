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
package io.trino.hive.formats.line.sequence;

import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.ReadWriteUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.function.Function;

import static io.trino.hive.formats.ReadWriteUtils.readVInt;
import static io.trino.hive.formats.ReadWriteUtils.writeVInt;
import static java.lang.Integer.reverseBytes;
import static java.lang.Math.toIntExact;

public enum ValueType
{
    TEXT("org.apache.hadoop.io.Text", ReadWriteUtils::computeVIntLength) {
        public int readLengthField(DataInput input)
                throws IOException
        {
            return toIntExact(readVInt(input));
        }

        public void writeLengthField(SliceOutput output, int length)
        {
            writeVInt(output, length);
        }
    },
    BYTES("org.apache.hadoop.io.BytesWritable", i -> 4) {
        public int readLengthField(DataInput input)
                throws IOException
        {
            return reverseBytes(input.readInt());
        }

        public void writeLengthField(SliceOutput output, int length)
        {
            output.writeInt(reverseBytes(length));
        }
    };

    private final String className;
    private final Function<Integer, Integer> lengthOfLengthField;

    ValueType(String className, Function<Integer, Integer> lengthOfLengthField)
    {
        this.className = className;
        this.lengthOfLengthField = lengthOfLengthField;
    }

    public String getClassName()
    {
        return className;
    }

    public int computeLengthOfLengthField(int valueLength)
    {
        return lengthOfLengthField.apply(valueLength);
    }

    public abstract int readLengthField(DataInput input)
            throws IOException;

    public abstract void writeLengthField(SliceOutput output, int length);
}
