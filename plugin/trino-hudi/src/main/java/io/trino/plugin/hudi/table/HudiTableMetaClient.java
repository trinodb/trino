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
package io.trino.plugin.hudi.table;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hudi.config.HudiTableConfig;
import io.trino.plugin.hudi.model.HudiInstant;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.plugin.hudi.timeline.HudiActiveTimeline;
import io.trino.plugin.hudi.timeline.HudiTimeline;
import io.trino.plugin.hudi.timeline.TimelineLayout;
import io.trino.plugin.hudi.timeline.TimelineLayoutVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hudi.HudiUtil.hudiMetadataExists;
import static java.util.Objects.requireNonNull;

public class HudiTableMetaClient
{
    public static final String METAFOLDER_NAME = ".hoodie";
    public static final String SEPARATOR = "/";
    public static final String AUXILIARYFOLDER_NAME = METAFOLDER_NAME + SEPARATOR + ".aux";
    public static final String SCHEMA_FOLDER_NAME = ".schema";

    private final Location metaPath;
    private final Location basePath;
    private final HudiTableType tableType;
    private final TimelineLayoutVersion timelineLayoutVersion;
    private final HudiTableConfig tableConfig;
    private final TrinoFileSystem fileSystem;

    private HudiActiveTimeline activeTimeline;

    protected HudiTableMetaClient(
            TrinoFileSystem fileSystem,
            Location basePath,
            Optional<TimelineLayoutVersion> layoutVersion)
    {
        this.metaPath = requireNonNull(basePath, "basePath is null").appendPath(METAFOLDER_NAME);
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        checkArgument(hudiMetadataExists(fileSystem, basePath), "Could not check if %s is a valid table", basePath);
        this.basePath = basePath;

        this.tableConfig = new HudiTableConfig(fileSystem, metaPath);
        this.tableType = tableConfig.getTableType();
        // TODO: Migrate Timeline objects
        Optional<TimelineLayoutVersion> tableConfigVersion = tableConfig.getTimelineLayoutVersion();
        if (layoutVersion.isPresent() && tableConfigVersion.isPresent()) {
            // Ensure layout version passed in config is not lower than the one seen in hoodie.properties
            checkArgument(layoutVersion.get().compareTo(tableConfigVersion.get()) >= 0,
                    "Layout Version defined in hoodie properties has higher version (%s) than the one passed in config (%s)",
                    tableConfigVersion.get(),
                    layoutVersion.get());
        }
        this.timelineLayoutVersion = layoutVersion.orElseGet(() -> tableConfig.getTimelineLayoutVersion().orElseThrow());
    }

    public HudiTableConfig getTableConfig()
    {
        return tableConfig;
    }

    public HudiTableType getTableType()
    {
        return tableType;
    }

    public HudiTimeline getCommitsTimeline()
    {
        return switch (this.getTableType()) {
            case COPY_ON_WRITE -> getActiveTimeline().getCommitTimeline();
            case MERGE_ON_READ ->
                // We need to include the parquet files written out in delta commits
                // Include commit action to be able to start doing a MOR over a COW table - no
                // migration required
                    getActiveTimeline().getCommitsTimeline();
        };
    }

    public synchronized HudiActiveTimeline getActiveTimeline()
    {
        if (activeTimeline == null) {
            activeTimeline = new HudiActiveTimeline(this);
        }
        return activeTimeline;
    }

    public TimelineLayoutVersion getTimelineLayoutVersion()
    {
        return timelineLayoutVersion;
    }

    public Location getBasePath()
    {
        return basePath;
    }

    public Location getMetaPath()
    {
        return metaPath;
    }

    public TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    public String getMetaAuxiliaryPath()
    {
        return basePath + SEPARATOR + AUXILIARYFOLDER_NAME;
    }

    public String getSchemaFolderName()
    {
        return metaPath.appendPath(SCHEMA_FOLDER_NAME).path();
    }

    private static HudiTableMetaClient newMetaClient(
            TrinoFileSystem fileSystem,
            Location basePath)
    {
        return new HudiTableMetaClient(fileSystem, basePath, Optional.of(TimelineLayoutVersion.CURRENT_LAYOUT_VERSION));
    }

    public List<HudiInstant> scanHoodieInstantsFromFileSystem(Set<String> includedExtensions,
            boolean applyLayoutVersionFilters)
            throws IOException
    {
        Stream<HudiInstant> instantStream = scanFiles(location -> {
            String extension = HudiInstant.getTimelineFileExtension(location.fileName());
            return includedExtensions.contains(extension);
        }).stream().map(HudiInstant::new);

        if (applyLayoutVersionFilters) {
            instantStream = TimelineLayout.getLayout(getTimelineLayoutVersion()).filterHoodieInstants(instantStream);
        }
        return instantStream.sorted().collect(Collectors.toList());
    }

    private List<FileEntry> scanFiles(Predicate<Location> pathPredicate)
            throws IOException
    {
        FileIterator fileIterator = fileSystem.listFiles(metaPath);
        List<FileEntry> result = new ArrayList<>();
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (pathPredicate.test(fileEntry.location())) {
                result.add(fileEntry);
            }
        }
        return result;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private TrinoFileSystem fileSystem;
        private Location basePath;

        public Builder setTrinoFileSystem(TrinoFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
            return this;
        }

        public Builder setBasePath(Location basePath)
        {
            this.basePath = basePath;
            return this;
        }

        public HudiTableMetaClient build()
        {
            return newMetaClient(fileSystem, basePath);
        }
    }
}
