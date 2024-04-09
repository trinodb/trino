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
package org.apache.parquet.hadoop.metadata;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.Set;

public class ColumnChunkProperties
{
    private static final Canonicalizer<ColumnChunkProperties> PROPERTIES = new Canonicalizer<>();

    @Deprecated
    public static ColumnChunkProperties get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings)
    {
        return get(path, new PrimitiveType(Type.Repetition.OPTIONAL, type, ""), codec, encodings);
    }

    public static ColumnChunkProperties get(ColumnPath path, PrimitiveType type, CompressionCodecName codec, Set<Encoding> encodings)
    {
        return PROPERTIES.canonicalize(new ColumnChunkProperties(codec, path, type, encodings));
    }

    private final CompressionCodecName codec;
    private final ColumnPath path;
    private final PrimitiveType type;
    private final Set<Encoding> encodings;

    private ColumnChunkProperties(CompressionCodecName codec, ColumnPath path, PrimitiveType type, Set<Encoding> encodings)
    {
        this.codec = codec;
        this.path = path;
        this.type = type;
        this.encodings = encodings;
    }

    public CompressionCodecName getCodec()
    {
        return codec;
    }

    public ColumnPath getPath()
    {
        return path;
    }

    @Deprecated
    public PrimitiveTypeName getType()
    {
        return type.getPrimitiveTypeName();
    }

    public PrimitiveType getPrimitiveType()
    {
        return type;
    }

    public Set<Encoding> getEncodings()
    {
        return encodings;
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof ColumnChunkProperties other) &&
                (other.codec == codec) &&
                other.path.equals(path) &&
                other.type.equals(type) &&
                (other.encodings.size() == encodings.size()) &&
                other.encodings.containsAll(encodings);
    }

    @Override
    public int hashCode()
    {
        return codec.hashCode() ^
                path.hashCode() ^
                type.hashCode() ^
                Arrays.hashCode(encodings.toArray());
    }

    @Override
    public String toString()
    {
        return codec + " " + path + " " + type + "  " + encodings;
    }
}
