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
package io.trino.plugin.hudi.model;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileEntry;
import io.trino.plugin.hudi.timeline.HudiTimeline;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import static io.trino.plugin.hudi.timeline.HudiTimeline.INFLIGHT_EXTENSION;
import static io.trino.plugin.hudi.timeline.HudiTimeline.REQUESTED_EXTENSION;
import static java.util.Objects.requireNonNull;

public class HudiInstant
        implements Comparable<HudiInstant>
{
    public enum State
    {
        // Requested State (valid state for Compaction)
        REQUESTED,
        // Inflight instant
        INFLIGHT,
        // Committed instant
        COMPLETED,
        // Invalid instant
        NIL
    }

    private static final Map<String, String> COMPARABLE_ACTIONS =
            ImmutableMap.of(HudiTimeline.COMPACTION_ACTION, HudiTimeline.COMMIT_ACTION);

    private static final Comparator<HudiInstant> ACTION_COMPARATOR =
            Comparator.comparing(instant -> getComparableAction(instant.getAction()));

    private static final Comparator<HudiInstant> COMPARATOR = Comparator.comparing(HudiInstant::getTimestamp)
            .thenComparing(ACTION_COMPARATOR).thenComparing(HudiInstant::getState);

    public static String getComparableAction(String action)
    {
        return COMPARABLE_ACTIONS.getOrDefault(action, action);
    }

    public static String getTimelineFileExtension(String fileName)
    {
        requireNonNull(fileName);
        int dotIndex = fileName.indexOf('.');
        return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    private HudiInstant.State state = HudiInstant.State.COMPLETED;
    private String action;
    private String timestamp;

    /**
     * Load the instant from the meta FileStatus.
     */
    public HudiInstant(FileEntry fileEntry)
    {
        // First read the instant timestamp. [==>20170101193025<==].commit
        String fileName = fileEntry.location().fileName();
        String fileExtension = getTimelineFileExtension(fileName);
        timestamp = fileName.replace(fileExtension, "");

        // Next read the action for this marker
        action = fileExtension.replaceFirst(".", "");
        if (action.equals("inflight")) {
            // This is to support backwards compatibility on how in-flight commit files were written
            // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
            action = "commit";
            state = HudiInstant.State.INFLIGHT;
        }
        else if (action.contains(INFLIGHT_EXTENSION)) {
            state = HudiInstant.State.INFLIGHT;
            action = action.replace(INFLIGHT_EXTENSION, "");
        }
        else if (action.contains(REQUESTED_EXTENSION)) {
            state = HudiInstant.State.REQUESTED;
            action = action.replace(REQUESTED_EXTENSION, "");
        }
    }

    public HudiInstant(HudiInstant.State state, String action, String timestamp)
    {
        this.state = state;
        this.action = action;
        this.timestamp = timestamp;
    }

    public boolean isCompleted()
    {
        return state == HudiInstant.State.COMPLETED;
    }

    public boolean isInflight()
    {
        return state == HudiInstant.State.INFLIGHT;
    }

    public boolean isRequested()
    {
        return state == HudiInstant.State.REQUESTED;
    }

    public String getAction()
    {
        return action;
    }

    public String getTimestamp()
    {
        return timestamp;
    }

    public String getFileName()
    {
        if (HudiTimeline.COMMIT_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightCommitFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedCommitFileName(timestamp)
                    : HudiTimeline.makeCommitFileName(timestamp);
        }
        else if (HudiTimeline.CLEAN_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightCleanerFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedCleanerFileName(timestamp)
                    : HudiTimeline.makeCleanerFileName(timestamp);
        }
        else if (HudiTimeline.ROLLBACK_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightRollbackFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedRollbackFileName(timestamp)
                    : HudiTimeline.makeRollbackFileName(timestamp);
        }
        else if (HudiTimeline.SAVEPOINT_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightSavePointFileName(timestamp)
                    : HudiTimeline.makeSavePointFileName(timestamp);
        }
        else if (HudiTimeline.DELTA_COMMIT_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightDeltaFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedDeltaFileName(timestamp)
                    : HudiTimeline.makeDeltaFileName(timestamp);
        }
        else if (HudiTimeline.COMPACTION_ACTION.equals(action)) {
            if (isInflight()) {
                return HudiTimeline.makeInflightCompactionFileName(timestamp);
            }
            else if (isRequested()) {
                return HudiTimeline.makeRequestedCompactionFileName(timestamp);
            }
            else {
                return HudiTimeline.makeCommitFileName(timestamp);
            }
        }
        else if (HudiTimeline.RESTORE_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightRestoreFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedRestoreFileName(timestamp)
                    : HudiTimeline.makeRestoreFileName(timestamp);
        }
        else if (HudiTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightReplaceFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedReplaceFileName(timestamp)
                    : HudiTimeline.makeReplaceFileName(timestamp);
        }
        else if (HudiTimeline.INDEXING_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightIndexFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestedIndexFileName(timestamp)
                    : HudiTimeline.makeIndexCommitFileName(timestamp);
        }
        else if (HudiTimeline.SCHEMA_COMMIT_ACTION.equals(action)) {
            return isInflight() ? HudiTimeline.makeInflightSchemaFileName(timestamp)
                    : isRequested() ? HudiTimeline.makeRequestSchemaFileName(timestamp)
                    : HudiTimeline.makeSchemaFileName(timestamp);
        }
        throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
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
        HudiInstant that = (HudiInstant) o;
        return state == that.state && Objects.equals(action, that.action) && Objects.equals(timestamp, that.timestamp);
    }

    public HudiInstant.State getState()
    {
        return state;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, action, timestamp);
    }

    @Override
    public int compareTo(HudiInstant o)
    {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public String toString()
    {
        return "[" + ((isInflight() || isRequested()) ? "==>" : "") + timestamp + "__" + action + "__" + state + "]";
    }
}
