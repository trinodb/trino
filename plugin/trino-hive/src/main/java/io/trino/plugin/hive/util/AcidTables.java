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
package io.trino.plugin.hive.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

import static io.trino.hive.thrift.metastore.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

// based on org.apache.hadoop.hive.ql.io.AcidUtils
public final class AcidTables
{
    private AcidTables() {}

    public static boolean isInsertOnlyTable(Map<String, String> parameters)
    {
        return "insert_only".equalsIgnoreCase(parameters.get(TABLE_TRANSACTIONAL_PROPERTIES));
    }

    public static boolean isTransactionalTable(Map<String, String> parameters)
    {
        return "true".equalsIgnoreCase(parameters.get(TABLE_IS_TRANSACTIONAL)) ||
                "true".equalsIgnoreCase(parameters.get(TABLE_IS_TRANSACTIONAL.toUpperCase(ENGLISH)));
    }

    public static boolean isFullAcidTable(Map<String, String> parameters)
    {
        return isTransactionalTable(parameters) && !isInsertOnlyTable(parameters);
    }

    public static Path bucketFileName(Path subdir, int bucket)
    {
        return new Path(subdir, "bucket_%05d".formatted(bucket));
    }

    public static String deltaSubdir(long writeId, int statementId)
    {
        return "delta_%07d_%07d_%04d".formatted(writeId, writeId, statementId);
    }

    public static String deleteDeltaSubdir(long writeId, int statementId)
    {
        return "delete_" + deltaSubdir(writeId, statementId);
    }

    public static void writeAcidVersionFile(FileSystem fs, Path deltaOrBaseDir)
            throws IOException
    {
        Path formatFile = versionFilePath(deltaOrBaseDir);
        if (!fs.exists(formatFile)) {
            try (var out = fs.create(formatFile, false)) {
                out.write('2');
            }
        }
    }

    public static int readAcidVersionFile(FileSystem fs, Path deltaOrBaseDir)
            throws IOException
    {
        Path formatFile = versionFilePath(deltaOrBaseDir);
        if (!fs.exists(formatFile)) {
            return 0;
        }
        try (var in = fs.open(formatFile)) {
            byte[] bytes = new byte[1];
            if (in.read(bytes) == 1) {
                return parseInt(new String(bytes, UTF_8));
            }
            return 0;
        }
    }

    private static Path versionFilePath(Path deltaOrBase)
    {
        return new Path(deltaOrBase, "_orc_acid_version");
    }
}
