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
package io.trino.plugin.hudi.files;

import io.trino.filesystem.Location;
import io.trino.spi.TrinoException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.model.HudiFileFormat.HOODIE_LOG;

public final class FSUtils
{
    private FSUtils()
    {
    }

    public static final Pattern LOG_FILE_PATTERN =
            Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)(_(([0-9]*)-([0-9]*)-([0-9]*)))?");

    public static String getFileIdFromLogPath(Location location)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(location.fileName());
        if (!matcher.find()) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid LogFile " + location);
        }
        return matcher.group(1);
    }

    public static String getBaseCommitTimeFromLogPath(Location location)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(location.fileName());
        if (!matcher.find()) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid LogFile " + location);
        }
        return matcher.group(2);
    }

    public static boolean isLogFile(String fileName)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
        return matcher.find() && fileName.contains(HOODIE_LOG.getFileExtension());
    }

    public static int getFileVersionFromLog(Location logLocation)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(logLocation.fileName());
        if (!matcher.find()) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid location " + logLocation);
        }
        return Integer.parseInt(matcher.group(4));
    }

    public static String getWriteTokenFromLogPath(Location location)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(location.fileName());
        if (!matcher.find()) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid location " + location);
        }
        return matcher.group(6);
    }

    public static String getCommitTime(String fullFileName)
    {
        Matcher matcher = LOG_FILE_PATTERN.matcher(fullFileName);
        if (matcher.find() && fullFileName.contains(HOODIE_LOG.getFileExtension())) {
            return fullFileName.split("_")[1].split("\\.")[0];
        }
        return fullFileName.split("_")[2].split("\\.")[0];
    }

    public static Location getPartitionLocation(Location baseLocation, String partitionPath)
    {
        return isNullOrEmpty(partitionPath) ? baseLocation : baseLocation.appendPath(partitionPath);
    }
}
