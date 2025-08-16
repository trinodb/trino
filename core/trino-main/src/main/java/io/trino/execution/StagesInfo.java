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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class StagesInfo
{
    private final StageId outputStageId;
    private final List<StageInfo> stages;
    private final Map<StageId, StageInfo> stagesById;

    @JsonCreator
    public StagesInfo(StageId outputStageId, List<StageInfo> stages)
    {
        requireNonNull(outputStageId, "outputStageId is null");
        requireNonNull(stages, "stages is null");
        checkArgument(!stages.isEmpty(), "stages cannot be empty");
        this.stages = ImmutableList.copyOf(stages);
        this.stagesById = Maps.uniqueIndex(stages, StageInfo::getStageId);
        checkArgument(stagesById.containsKey(outputStageId), "output stage not found in list of stages");
        this.outputStageId = outputStageId;

        Set<StageId> stageIdsFromSubStages = stages.stream().flatMap(stageInfo -> stageInfo.getSubStages().stream()).collect(toImmutableSet());
        Set<StageId> stageIdsFromStageInfos = stages.stream().map(StageInfo::getStageId).collect(toImmutableSet());
        checkArgument(stageIdsFromStageInfos.size() == stages.size(), "found non-uniq stage ids");
        checkArgument(stageIdsFromStageInfos.containsAll(stageIdsFromSubStages), "unknown stage ids referenced in substages");
    }

    @JsonProperty
    public StageId getOutputStageId()
    {
        return outputStageId;
    }

    @JsonProperty
    public List<StageInfo> getStages()
    {
        return stages;
    }

    public StagesInfo pruneDigests()
    {
        return new StagesInfo(
                outputStageId,
                stages.stream().map(StageInfo::pruneDigests).collect(toImmutableList()));
    }

    @JsonIgnore
    public StageInfo getOutputStage()
    {
        return stagesById.get(outputStageId);
    }

    /**
     * Shallow list of substages of stage referenced by id
     */
    @JsonIgnore
    public List<StageInfo> getSubStages(StageId stageId)
    {
        StageInfo stageInfo = stagesById.get(stageId);
        checkArgument(stageInfo != null, "Stage %s not found", stageId);
        return stageInfo.getSubStages().stream()
                .map(stagesById::get)
                .collect(toImmutableList());
    }

    @JsonIgnore
    public List<StageInfo> getSubStagesDeepPreOrder(StageId stageId)
    {
        return getSubStagesDeepPreOrder(stageId, false);
    }

    @JsonIgnore
    public List<StageInfo> getSubStagesDeepPreOrder(StageId root, boolean includeRoot)
    {
        StageInfo stageInfo = stagesById.get(root);
        checkArgument(stageInfo != null, "stage %s not found", root);

        ImmutableSet.Builder<StageId> subStagesIds = ImmutableSet.builder();
        if (includeRoot) {
            subStagesIds.add(root);
        }
        collectSubStageIdsPreOrder(stageInfo, subStagesIds);

        return subStagesIds.build().stream().map(stagesById::get).collect(toImmutableList());
    }

    private void collectSubStageIdsPreOrder(StageInfo stageInfo, ImmutableSet.Builder collector)
    {
        stageInfo.getSubStages().stream().forEach(subStageId -> {
            collector.add(subStageId);
            StageInfo subStage = stagesById.get(subStageId);
            collectSubStageIdsPreOrder(subStage, collector);
        });
    }

    @JsonIgnore
    public List<StageInfo> getSubStagesDeepPostOrder(StageId stageId)
    {
        return getSubStagesDeepPostOrder(stageId, false);
    }

    @JsonIgnore
    public List<StageInfo> getSubStagesDeepPostOrder(StageId root, boolean includeRoot)
    {
        StageInfo stageInfo = stagesById.get(root);
        checkArgument(stageInfo != null, "stage %s not found", root);

        ImmutableSet.Builder<StageId> subStagesIds = ImmutableSet.builder();
        collectSubStageIdsPostOrder(stageInfo, subStagesIds);
        if (includeRoot) {
            subStagesIds.add(root);
        }

        return subStagesIds.build().stream().map(stagesById::get).collect(toImmutableList());
    }

    private void collectSubStageIdsPostOrder(StageInfo stageInfo, ImmutableSet.Builder collector)
    {
        stageInfo.getSubStages().stream().forEach(subStageId -> {
            StageInfo subStage = stagesById.get(subStageId);
            collectSubStageIdsPostOrder(subStage, collector);
            collector.add(subStageId);
        });
    }

    public static List<StageInfo> getAllStages(Optional<StagesInfo> stages)
    {
        return stages.map(StagesInfo::getStages).orElse(ImmutableList.of());
    }
}
