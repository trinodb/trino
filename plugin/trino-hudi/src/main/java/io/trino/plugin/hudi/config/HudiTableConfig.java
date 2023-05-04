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
package io.trino.plugin.hudi.config;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hudi.model.HudiFileFormat;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.plugin.hudi.timeline.TimelineLayoutVersion;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static java.lang.String.format;

public class HudiTableConfig
{
    public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
    public static final String HOODIE_PROPERTIES_FILE_BACKUP = "hoodie.properties.backup";
    public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
    public static final String HOODIE_TABLE_TYPE_KEY = "hoodie.table.type";
    public static final String HOODIE_TABLE_BASE_FILE_FORMAT = "hoodie.table.base.file.format";
    public static final String HOODIE_TIMELINE_LAYOUT_VERSION_KEY = "hoodie.timeline.layout.version";
    private final Properties properties;

    public HudiTableConfig(TrinoFileSystem fs, Location metaPath)
    {
        this.properties = new Properties();
        Location propertyPath = metaPath.appendPath(HOODIE_PROPERTIES_FILE);
        try {
            TrinoInputFile inputFile = fs.newInputFile(propertyPath);
            try (TrinoInputStream inputStream = inputFile.newStream()) {
                properties.load(inputStream);
            }
        }
        catch (IOException e) {
            if (!tryLoadingBackupPropertyFile(fs, metaPath)) {
                throw new TrinoException(HUDI_FILESYSTEM_ERROR, format("Could not load Hoodie properties from %s", propertyPath));
            }
        }
        checkArgument(properties.containsKey(HOODIE_TABLE_NAME_KEY) && properties.containsKey(HOODIE_TABLE_TYPE_KEY),
                "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, " +
                        "manually copy it to hoodie.properties and retry");
    }

    private boolean tryLoadingBackupPropertyFile(TrinoFileSystem fs, Location metaPath)
    {
        Location backupPath = metaPath.appendPath(HOODIE_PROPERTIES_FILE_BACKUP);
        try {
            FileIterator fileIterator = fs.listFiles(metaPath);
            while (fileIterator.hasNext()) {
                if (fileIterator.next().location().equals(backupPath)) {
                    // try the backup. this way no query ever fails if update fails midway.
                    TrinoInputFile inputFile = fs.newInputFile(backupPath);
                    try (TrinoInputStream inputStream = inputFile.newStream()) {
                        properties.load(inputStream);
                    }
                    return true;
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, "Failed to load Hudi properties from file: " + backupPath, e);
        }
        return false;
    }

    public HudiTableType getTableType()
    {
        return HudiTableType.valueOf(properties.getProperty(HOODIE_TABLE_TYPE_KEY));
    }

    public HudiFileFormat getBaseFileFormat()
    {
        if (properties.containsKey(HOODIE_TABLE_BASE_FILE_FORMAT)) {
            return HudiFileFormat.valueOf(properties.getProperty(HOODIE_TABLE_BASE_FILE_FORMAT));
        }
        return HudiFileFormat.PARQUET;
    }

    public Optional<TimelineLayoutVersion> getTimelineLayoutVersion()
    {
        return Optional.ofNullable(properties.getProperty(HOODIE_TIMELINE_LAYOUT_VERSION_KEY))
                .map(Integer::parseInt)
                .map(TimelineLayoutVersion::new);
    }
}
