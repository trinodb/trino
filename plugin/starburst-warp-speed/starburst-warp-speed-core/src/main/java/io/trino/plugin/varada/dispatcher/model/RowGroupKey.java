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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.varada.tools.util.PathUtils;
import io.varada.tools.util.StringUtils;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.StringJoiner;

public record RowGroupKey(
        @JsonProperty("schema_name") String schema,
        @JsonProperty("table_name") String table,
        @JsonProperty("file_path") String filePath,
        @JsonProperty("file_offset") long offset,
        @JsonProperty("length") long length,
        @JsonProperty("file_modified_time") long fileModifiedTime,
        @JsonProperty("deleted_files_hash") String deletedFilesHash,
        @JsonProperty("catalog_name") String catalogName)
        implements Serializable
{
    @JsonCreator
    public RowGroupKey {}

    @Override
    public String toString()
    {
        String filePathWithoutPrefix = filePath;
        if (filePathWithoutPrefix.contains("//")) {
            filePathWithoutPrefix = filePathWithoutPrefix.substring(filePathWithoutPrefix.indexOf("//") + "//".length());
        }

        StringJoiner stringJoiner = new StringJoiner(":");
        stringJoiner.add(schema)
                .add(table)
                .add(filePathWithoutPrefix)
                .add(Long.toString(offset))
                .add(Long.toString(length))
                .add(Long.toString(fileModifiedTime));
        if (!StringUtils.isEmpty(deletedFilesHash)) {
            stringJoiner.add(deletedFilesHash);
        }
        return stringJoiner.toString().replaceAll("\\s+", "_");
    }

    public String stringFileNameRepresentation(String storePath)
    {
        String filePathWithoutPrefix = filePath;
        if (filePathWithoutPrefix.contains("//")) {
            filePathWithoutPrefix = filePathWithoutPrefix.substring(filePathWithoutPrefix.indexOf("//") + "//".length());
        }

        Path path = Path.of(
                schema,
                table,
                filePathWithoutPrefix,
                Long.toString(offset),
                Long.toString(length),
                Long.toString(fileModifiedTime));
        if (!StringUtils.isEmpty(deletedFilesHash)) {
            path = Path.of(path.toString(), deletedFilesHash);
        }
        String fileName = path.toString().replaceAll("\\s+", "_");
        return PathUtils.getUriPath(storePath, catalogName, fileName);
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
        RowGroupKey that = (RowGroupKey) o;
        return offset == that.offset && length == that.length && fileModifiedTime == that.fileModifiedTime && Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(filePath, that.filePath) && Objects.equals(deletedFilesHash, that.deletedFilesHash) && Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table, filePath, offset, length, fileModifiedTime, deletedFilesHash, catalogName);
    }
}
