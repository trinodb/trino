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
package io.trino.plugin.lance.metadata;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import io.airlift.slice.Slice;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.spi.TrinoException;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.lance.file.LanceReader.toFields;
import static io.trino.plugin.lance.LanceErrorCode.LANCE_INVALID_METADATA;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;

public record Manifest(List<Field> fields, List<Fragment> fragments, long version, long maxFragmentId)
{
    private static final int MIN_FILE_SIZE = 16;
    private static final byte[] MAGIC = "LANC".getBytes(US_ASCII);

    public Manifest
    {
        fields = ImmutableList.copyOf(fields);
        fragments = ImmutableList.copyOf(fragments);
    }

    public static Manifest from(Slice slice)
    {
        checkArgument(slice.length() > MIN_FILE_SIZE, "Invalid manifest size: %s", slice);
        int length = slice.length();
        byte[] magic = slice.getBytes(length - MAGIC.length, MAGIC.length);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new TrinoException(LANCE_INVALID_METADATA, "Invalid MAGIC in manifest footer");
        }
        long position = slice.getLong(toIntExact(length - 16));
        int recordedLength = slice.getInt(toIntExact(position));
        if (recordedLength != length - position - 20) {
            throw new TrinoException(LANCE_INVALID_METADATA, "Invalid manifest proto message length: " + recordedLength);
        }
        build.buf.gen.lance.table.Manifest proto;
        try {
            proto = build.buf.gen.lance.table.Manifest.parseFrom(slice.slice(toIntExact(position + 4), recordedLength).toByteBuffer());
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(LANCE_INVALID_METADATA, e);
        }

        List<Fragment> fragments = proto.getFragmentsList().stream()
                .map(Fragment::from)
                .collect(toImmutableList());

        return new Manifest(toFields(proto.getFieldsList()),
                fragments,
                proto.getVersion(),
                proto.getMaxFragmentId());
    }
}
