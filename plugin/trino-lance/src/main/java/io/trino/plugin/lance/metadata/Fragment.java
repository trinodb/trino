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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record Fragment(long id, List<DataFile> files, long physicalRows)
{
    // TODO: support deletion files

    public Fragment
    {
        files = ImmutableList.copyOf(files);
    }

    public static Fragment from(build.buf.gen.lance.table.DataFragment proto)
    {
        if (proto.hasDeletionFile()) {
            throw new UnsupportedOperationException("Deletion files are not supported");
        }

        List<DataFile> files = proto.getFilesList().stream()
                .map(DataFile::from)
                .collect(toImmutableList());
        return new Fragment(proto.getId(), files, proto.getPhysicalRows());
    }

    public record DataFile(String path, List<Integer> fields, List<Integer> columnIndices, long fileMajorVersion, long fileMinorVersion)
    {
        public DataFile
        {
            requireNonNull(path, "path is null");
            requireNonNull(fields, "fields is null");
            requireNonNull(columnIndices, "columnIndices is null");
        }

        public static DataFile from(build.buf.gen.lance.table.DataFile proto)
        {
            return new DataFile(proto.getPath(),
                    proto.getFieldsList(),
                    proto.getColumnIndicesList(),
                    proto.getFileMajorVersion(),
                    proto.getFileMinorVersion());
        }
    }
}
