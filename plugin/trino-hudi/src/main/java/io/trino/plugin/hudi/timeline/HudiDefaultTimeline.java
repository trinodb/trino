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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hudi.model.HudiInstant;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hudi.timeline.HudiTimeline.compareTimestamps;

public class HudiDefaultTimeline
        implements HudiTimeline
{
    private List<HudiInstant> instants;
    protected transient Function<HudiInstant, Optional<byte[]>> details;

    public HudiDefaultTimeline(Stream<HudiInstant> instants, Function<HudiInstant, Optional<byte[]>> details)
    {
        this.details = details;
        setInstants(instants.collect(Collectors.toList()));
    }

    public void setInstants(List<HudiInstant> instants)
    {
        this.instants = ImmutableList.copyOf(instants);
    }

    public HudiDefaultTimeline()
    {
    }

    @Override
    public HudiTimeline filterCompletedInstants()
    {
        return new HudiDefaultTimeline(instants.stream().filter(HudiInstant::isCompleted), details);
    }

    @Override
    public HudiDefaultTimeline getWriteTimeline()
    {
        Set<String> validActions = ImmutableSet.of(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
        return new HudiDefaultTimeline(
                instants.stream().filter(s -> validActions.contains(s.getAction())),
                details);
    }

    @Override
    public HudiTimeline getCompletedReplaceTimeline()
    {
        return new HudiDefaultTimeline(
                instants.stream()
                        .filter(s -> s.getAction().equals(REPLACE_COMMIT_ACTION))
                        .filter(HudiInstant::isCompleted),
                details);
    }

    @Override
    public HudiTimeline filterPendingReplaceTimeline()
    {
        return new HudiDefaultTimeline(
                instants.stream().filter(s -> s.getAction().equals(HudiTimeline.REPLACE_COMMIT_ACTION) && !s.isCompleted()),
                details);
    }

    @Override
    public HudiTimeline filterPendingCompactionTimeline()
    {
        return new HudiDefaultTimeline(
                instants.stream().filter(s -> s.getAction().equals(HudiTimeline.COMPACTION_ACTION) && !s.isCompleted()),
                details);
    }

    public HudiTimeline getCommitsTimeline()
    {
        return getTimelineOfActions(ImmutableSet.of(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
    }

    public HudiTimeline getCommitTimeline()
    {
        return getTimelineOfActions(ImmutableSet.of(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
    }

    public HudiTimeline getTimelineOfActions(Set<String> actions)
    {
        return new HudiDefaultTimeline(
                getInstants().filter(s -> actions.contains(s.getAction())),
                this::getInstantDetails);
    }

    @Override
    public boolean empty()
    {
        return instants.stream().findFirst().isEmpty();
    }

    @Override
    public int countInstants()
    {
        return instants.size();
    }

    @Override
    public Optional<HudiInstant> firstInstant()
    {
        return instants.stream().findFirst();
    }

    @Override
    public Optional<HudiInstant> nthInstant(int n)
    {
        if (empty() || n >= countInstants()) {
            return Optional.empty();
        }
        return Optional.of(instants.get(n));
    }

    @Override
    public Optional<HudiInstant> lastInstant()
    {
        return empty() ? Optional.empty() : nthInstant(countInstants() - 1);
    }

    @Override
    public boolean containsOrBeforeTimelineStarts(String instant)
    {
        return instants.stream().anyMatch(s -> s.getTimestamp().equals(instant)) || isBeforeTimelineStarts(instant);
    }

    @Override
    public Stream<HudiInstant> getInstants()
    {
        return instants.stream();
    }

    @Override
    public boolean isBeforeTimelineStarts(String instant)
    {
        Optional<HudiInstant> firstNonSavepointCommit = getFirstNonSavepointCommit();
        return firstNonSavepointCommit.isPresent()
                && compareTimestamps(instant, LESSER_THAN, firstNonSavepointCommit.get().getTimestamp());
    }

    @Override
    public Optional<HudiInstant> getFirstNonSavepointCommit()
    {
        Optional<HudiInstant> firstCommit = firstInstant();
        Set<String> savepointTimestamps = instants.stream()
                .filter(entry -> entry.getAction().equals(SAVEPOINT_ACTION))
                .map(HudiInstant::getTimestamp)
                .collect(toImmutableSet());
        Optional<HudiInstant> firstNonSavepointCommit = firstCommit;
        if (!savepointTimestamps.isEmpty()) {
            // There are chances that there could be holes in the timeline due to archival and savepoint interplay.
            // So, the first non-savepoint commit is considered as beginning of the active timeline.
            firstNonSavepointCommit = instants.stream()
                    .filter(entry -> !savepointTimestamps.contains(entry.getTimestamp()))
                    .findFirst();
        }
        return firstNonSavepointCommit;
    }

    @Override
    public Optional<byte[]> getInstantDetails(HudiInstant instant)
    {
        return details.apply(instant);
    }

    @Override
    public String toString()
    {
        return this.getClass().getName() + ": " + instants.stream().map(Object::toString).collect(Collectors.joining(","));
    }
}
