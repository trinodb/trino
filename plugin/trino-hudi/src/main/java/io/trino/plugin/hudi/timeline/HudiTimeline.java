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
package io.trino.plugin.hudi.timeline;

import io.trino.plugin.hudi.model.HudiInstant;
import io.trino.plugin.hudi.model.HudiInstant.State;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static java.lang.String.join;

public interface HudiTimeline
{
    String COMMIT_ACTION = "commit";
    String DELTA_COMMIT_ACTION = "deltacommit";
    String CLEAN_ACTION = "clean";
    String ROLLBACK_ACTION = "rollback";
    String SAVEPOINT_ACTION = "savepoint";
    String REPLACE_COMMIT_ACTION = "replacecommit";
    String INFLIGHT_EXTENSION = ".inflight";
    // With Async Compaction, compaction instant can be in 3 states :
    // (compaction-requested), (compaction-inflight), (completed)
    String COMPACTION_ACTION = "compaction";
    String REQUESTED_EXTENSION = ".requested";
    String RESTORE_ACTION = "restore";
    String INDEXING_ACTION = "indexing";
    // only for schema save
    String SCHEMA_COMMIT_ACTION = "schemacommit";
    String COMMIT_EXTENSION = "." + COMMIT_ACTION;
    String DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION;
    String CLEAN_EXTENSION = "." + CLEAN_ACTION;
    String ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION;
    String SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION;
    // this is to preserve backwards compatibility on commit in-flight filenames
    String INFLIGHT_COMMIT_EXTENSION = INFLIGHT_EXTENSION;
    String REQUESTED_COMMIT_EXTENSION = "." + COMMIT_ACTION + REQUESTED_EXTENSION;
    String REQUESTED_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + REQUESTED_EXTENSION;
    String INFLIGHT_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + INFLIGHT_EXTENSION;
    String INFLIGHT_CLEAN_EXTENSION = "." + CLEAN_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_CLEAN_EXTENSION = "." + CLEAN_ACTION + REQUESTED_EXTENSION;
    String INFLIGHT_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + REQUESTED_EXTENSION;
    String INFLIGHT_SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_COMPACTION_SUFFIX = join("", COMPACTION_ACTION, REQUESTED_EXTENSION);
    String REQUESTED_COMPACTION_EXTENSION = join(".", REQUESTED_COMPACTION_SUFFIX);
    String INFLIGHT_COMPACTION_EXTENSION = join(".", COMPACTION_ACTION, INFLIGHT_EXTENSION);
    String REQUESTED_RESTORE_EXTENSION = "." + RESTORE_ACTION + REQUESTED_EXTENSION;
    String INFLIGHT_RESTORE_EXTENSION = "." + RESTORE_ACTION + INFLIGHT_EXTENSION;
    String RESTORE_EXTENSION = "." + RESTORE_ACTION;
    String INFLIGHT_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + REQUESTED_EXTENSION;
    String REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION;
    String INFLIGHT_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + REQUESTED_EXTENSION;
    String INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION;
    String SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION;
    String INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + INFLIGHT_EXTENSION;
    String REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + REQUESTED_EXTENSION;

    HudiTimeline filterCompletedInstants();

    HudiTimeline getWriteTimeline();

    HudiTimeline getCompletedReplaceTimeline();

    HudiTimeline filterPendingCompactionTimeline();

    HudiTimeline filterPendingReplaceTimeline();

    boolean empty();

    int countInstants();

    Optional<HudiInstant> firstInstant();

    Optional<HudiInstant> nthInstant(int n);

    Optional<HudiInstant> lastInstant();

    boolean containsOrBeforeTimelineStarts(String ts);

    Stream<HudiInstant> getInstants();

    boolean isBeforeTimelineStarts(String ts);

    Optional<HudiInstant> getFirstNonSavepointCommit();

    Optional<byte[]> getInstantDetails(HudiInstant instant);

    BiPredicate<String, String> LESSER_THAN_OR_EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
    BiPredicate<String, String> LESSER_THAN = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

    static boolean compareTimestamps(String commit1, BiPredicate<String, String> predicateToApply, String commit2)
    {
        return predicateToApply.test(commit1, commit2);
    }

    static HudiInstant getCompactionRequestedInstant(final String timestamp)
    {
        return new HudiInstant(State.REQUESTED, COMPACTION_ACTION, timestamp);
    }

    static String makeCommitFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.COMMIT_EXTENSION);
    }

    static String makeInflightCommitFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.INFLIGHT_COMMIT_EXTENSION);
    }

    static String makeRequestedCommitFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.REQUESTED_COMMIT_EXTENSION);
    }

    static String makeCleanerFileName(String instant)
    {
        return join("", instant, HudiTimeline.CLEAN_EXTENSION);
    }

    static String makeRequestedCleanerFileName(String instant)
    {
        return join("", instant, HudiTimeline.REQUESTED_CLEAN_EXTENSION);
    }

    static String makeInflightCleanerFileName(String instant)
    {
        return join("", instant, HudiTimeline.INFLIGHT_CLEAN_EXTENSION);
    }

    static String makeRollbackFileName(String instant)
    {
        return join("", instant, HudiTimeline.ROLLBACK_EXTENSION);
    }

    static String makeRequestedRollbackFileName(String instant)
    {
        return join("", instant, HudiTimeline.REQUESTED_ROLLBACK_EXTENSION);
    }

    static String makeRequestedRestoreFileName(String instant)
    {
        return join("", instant, HudiTimeline.REQUESTED_RESTORE_EXTENSION);
    }

    static String makeInflightRollbackFileName(String instant)
    {
        return join("", instant, HudiTimeline.INFLIGHT_ROLLBACK_EXTENSION);
    }

    static String makeInflightSavePointFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.INFLIGHT_SAVEPOINT_EXTENSION);
    }

    static String makeSavePointFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.SAVEPOINT_EXTENSION);
    }

    static String makeInflightDeltaFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
    }

    static String makeRequestedDeltaFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
    }

    static String makeInflightCompactionFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.INFLIGHT_COMPACTION_EXTENSION);
    }

    static String makeRequestedCompactionFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.REQUESTED_COMPACTION_EXTENSION);
    }

    static String makeRestoreFileName(String instant)
    {
        return join("", instant, HudiTimeline.RESTORE_EXTENSION);
    }

    static String makeInflightRestoreFileName(String instant)
    {
        return join("", instant, HudiTimeline.INFLIGHT_RESTORE_EXTENSION);
    }

    static String makeReplaceFileName(String instant)
    {
        return join("", instant, HudiTimeline.REPLACE_COMMIT_EXTENSION);
    }

    static String makeInflightReplaceFileName(String instant)
    {
        return join("", instant, HudiTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
    }

    static String makeRequestedReplaceFileName(String instant)
    {
        return join("", instant, HudiTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION);
    }

    static String makeDeltaFileName(String instantTime)
    {
        return instantTime + HudiTimeline.DELTA_COMMIT_EXTENSION;
    }

    static String makeIndexCommitFileName(String instant)
    {
        return join("", instant, HudiTimeline.INDEX_COMMIT_EXTENSION);
    }

    static String makeInflightIndexFileName(String instant)
    {
        return join("", instant, HudiTimeline.INFLIGHT_INDEX_COMMIT_EXTENSION);
    }

    static String makeRequestedIndexFileName(String instant)
    {
        return join("", instant, HudiTimeline.REQUESTED_INDEX_COMMIT_EXTENSION);
    }

    static String makeSchemaFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.SAVE_SCHEMA_ACTION_EXTENSION);
    }

    static String makeInflightSchemaFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION);
    }

    static String makeRequestSchemaFileName(String instantTime)
    {
        return join("", instantTime, HudiTimeline.REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION);
    }
}
