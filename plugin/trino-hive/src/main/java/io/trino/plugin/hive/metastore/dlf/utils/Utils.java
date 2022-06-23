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
package io.trino.plugin.hive.metastore.dlf.utils;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public class Utils
{
    private static final Logger logger = Logger.get(Utils.class);

    private Utils() {}

    static String realMessage(boolean enableFsOperation)
    {
        return enableFsOperation ? "real" : "mock";
    }

    public static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals("MANAGED_TABLE");
    }

    public static boolean makeDir(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, boolean enableFileOperation)
    {
        boolean madeDir = false;
        long startTime = System.currentTimeMillis();
        try {
            if (enableFileOperation) {
                madeDir = hdfsEnvironment.getFileSystem(context, path).mkdirs(path);
            }
            else {
                madeDir = true;
            }

            return madeDir;
        }
        catch (Exception e) {
            logger.error(e, "Failed to mk path : " + path);
            return false;
        }
        finally {
            final boolean madeDirFinal = madeDir;
            ProxyLogUtils.printLog(() -> logger.info("alibaba-dlf.fs.%s.mkdir, result:%s, cost:%dms, path:%s", realMessage(enableFileOperation), madeDirFinal, System.currentTimeMillis() - startTime, path));
        }
    }

    public static void deleteDir(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, boolean recursive, boolean enableFileOperation)
    {
        boolean deleteDir = false;
        long startTime = System.currentTimeMillis();
        try {
            if (enableFileOperation) {
                deleteDir = hdfsEnvironment.getFileSystem(context, path).delete(path, recursive);
            }
            else {
                deleteDir = true;
            }
        }
        catch (Exception e) {
            // don't fail if unable to delete path
            logger.error(e, "Failed to delete path: " + path);
        }
        finally {
            final boolean deleteDirFinal = deleteDir;
            ProxyLogUtils.printLog(() -> logger.info("alibaba-dlf.fs.%s.deleteDir, result:%s, cost:%dms , path:%s, recursive:%s",
                                                     realMessage(enableFileOperation), deleteDirFinal, System.currentTimeMillis() - startTime, path, recursive));
        }
    }

    public static String lowerCaseConvertPartName(String partName)
    {
        try {
            boolean isFirst = true;
            Map<String, String> partSpec = Warehouse.makeEscSpecFromName(partName);
            String convertedPartName = new String();

            for (Map.Entry<String, String> entry : partSpec.entrySet()) {
                String partColName = entry.getKey();
                String partColVal = entry.getValue();

                if (!isFirst) {
                    convertedPartName += "/";
                }
                else {
                    isFirst = false;
                }
                convertedPartName += partColName.toLowerCase(Locale.ENGLISH) + "=" + partColVal;
            }
            return convertedPartName;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public static List<String> getPartValsFromName(Table t, String partName)
            throws MetaException, InvalidObjectException
    {
        checkArgument(t != null, "Table can not be null");
        // Unescape the partition name
        LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

        List<String> partVals = new ArrayList<>();
        for (Column field : t.getPartitionColumns()) {
            String key = field.getName();
            String val = hm.get(key);
            if (val == null) {
                throw new InvalidObjectException("incomplete partition name - missing " + key);
            }
            partVals.add(val);
        }
        return partVals;
    }

    public static boolean isExternalTable(Table table)
    {
        return TableType.EXTERNAL_TABLE.name().equals(table.getTableType()) || isExternalTableSetInParameters(table);
    }

    public static boolean isExternalTableSetInParameters(Table table)
    {
        if (table.getParameters() == null) {
            return false;
        }
        return Boolean.parseBoolean(table.getParameters().get("EXTERNAL"));
    }

    public static boolean renameFs(FileSystem fs, Path src, Path dest, boolean enableFsOperation) throws IOException
    {
        long startTime = System.currentTimeMillis();
        boolean rename = false;
        try {
            if (enableFsOperation) {
                rename = fs.rename(src, dest);
            }
            else {
                rename = true;
            }

            return rename;
        }
        finally {
            final boolean renameFinal = rename;
            ProxyLogUtils.printLog(() -> logger.info("alibaba-dlf.fs.%s.renameFs, result:%s, cost:%dms, src:%s, dest:%s",
                                                     realMessage(enableFsOperation), renameFinal, System.currentTimeMillis() - startTime, src, dest));
        }
    }

    /**
     * Uses the scheme and authority of the object's current location and the path constructed
     * using the object's new name to construct a path for the object's new location.
     */
    public static Path constructRenamedPath(Path newPath, Path currentPath)
    {
        URI currentUri = currentPath.toUri();
        return new Path(currentUri.getScheme(), currentUri.getAuthority(), newPath.toUri().getPath());
    }
}
