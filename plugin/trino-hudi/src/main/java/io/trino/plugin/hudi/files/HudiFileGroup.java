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

import io.trino.plugin.hudi.model.HudiInstant;
import io.trino.plugin.hudi.timeline.HudiTimeline;

import java.util.Comparator;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hudi.timeline.HudiTimeline.LESSER_THAN_OR_EQUALS;
import static io.trino.plugin.hudi.timeline.HudiTimeline.compareTimestamps;
import static java.util.Objects.requireNonNull;

public class HudiFileGroup
{
    public static Comparator<String> getReverseCommitTimeComparator()
    {
        return Comparator.reverseOrder();
    }

    private final HudiFileGroupId fileGroupId;

    private final TreeMap<String, FileSlice> fileSlices;

    private final HudiTimeline timeline;

    private final Optional<HudiInstant> lastInstant;

    public HudiFileGroup(String partitionPath, String id, HudiTimeline timeline)
    {
        this(new HudiFileGroupId(partitionPath, id), timeline);
    }

    public HudiFileGroup(HudiFileGroupId fileGroupId, HudiTimeline timeline)
    {
        this.fileGroupId = requireNonNull(fileGroupId, "fileGroupId is null");
        this.fileSlices = new TreeMap<>(HudiFileGroup.getReverseCommitTimeComparator());
        this.lastInstant = timeline.lastInstant();
        this.timeline = timeline;
    }

    public void addNewFileSliceAtInstant(String baseInstantTime)
    {
        if (!fileSlices.containsKey(baseInstantTime)) {
            fileSlices.put(baseInstantTime, new FileSlice(baseInstantTime));
        }
    }

    public void addBaseFile(HudiBaseFile dataFile)
    {
        if (!fileSlices.containsKey(dataFile.getCommitTime())) {
            fileSlices.put(dataFile.getCommitTime(), new FileSlice(dataFile.getCommitTime()));
        }
        fileSlices.get(dataFile.getCommitTime()).setBaseFile(dataFile);
    }

    public void addLogFile(HudiLogFile logFile)
    {
        if (!fileSlices.containsKey(logFile.getBaseCommitTime())) {
            fileSlices.put(logFile.getBaseCommitTime(), new FileSlice(logFile.getBaseCommitTime()));
        }
        fileSlices.get(logFile.getBaseCommitTime()).addLogFile(logFile);
    }

    public String getPartitionPath()
    {
        return fileGroupId.getPartitionPath();
    }

    public HudiFileGroupId getFileGroupId()
    {
        return fileGroupId;
    }

    private boolean isFileSliceCommitted(FileSlice slice)
    {
        if (!compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, lastInstant.get().getTimestamp())) {
            return false;
        }

        return timeline.containsOrBeforeTimelineStarts(slice.getBaseInstantTime());
    }

    public Stream<FileSlice> getAllFileSlices()
    {
        if (!timeline.empty()) {
            return fileSlices.values().stream().filter(this::isFileSliceCommitted);
        }
        return Stream.empty();
    }

    public Stream<HudiBaseFile> getAllBaseFiles()
    {
        return getAllFileSlices().filter(slice -> slice.getBaseFile().isPresent()).map(slice -> slice.getBaseFile().get());
    }

    public Stream<FileSlice> getAllFileSlicesBeforeOn(String maxInstantTime)
    {
        return fileSlices.values().stream().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, maxInstantTime));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileGroupId", fileGroupId)
                .add("fileSlices", fileSlices)
                .add("timeline", timeline)
                .add("lastInstant", lastInstant)
                .toString();
    }

    public HudiTimeline getTimeline()
    {
        return timeline;
    }
}
