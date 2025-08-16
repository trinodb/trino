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
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class BasicStagesInfo
{
    private final StageId outputStageId;
    private final List<BasicStageInfo> stages;
    private final Map<StageId, BasicStageInfo> stagesById;

    public BasicStagesInfo(StagesInfo stages)
    {
        this(stages.getOutputStageId(),
                stages.getStages().stream()
                        .map(BasicStageInfo::new)
                        .collect(toImmutableList()));
    }

    @JsonCreator
    public BasicStagesInfo(StageId outputStageId, List<BasicStageInfo> stages)
    {
        requireNonNull(outputStageId, "outputStageId is null");
        requireNonNull(stages, "stages is null");
        checkArgument(!stages.isEmpty(), "stages cannot be empty");
        this.stages = ImmutableList.copyOf(stages);
        this.stagesById = Maps.uniqueIndex(stages, BasicStageInfo::getStageId);
        checkArgument(stagesById.containsKey(outputStageId), "output stage not found in list of stages");
        this.outputStageId = outputStageId;

        Set<StageId> stageIdsFromSubStages = stages.stream().flatMap(stageInfo -> stageInfo.getSubStages().stream()).collect(toImmutableSet());
        Set<StageId> stageIdsFromStageInfos = stages.stream().map(BasicStageInfo::getStageId).collect(toImmutableSet());
        checkArgument(stageIdsFromStageInfos.size() == stages.size(), "found non-uniq stage ids");
        checkArgument(stageIdsFromStageInfos.containsAll(stageIdsFromSubStages), "unknown stage ids referenced in substages");
    }

    @JsonProperty
    public StageId getOutputStageId()
    {
        return outputStageId;
    }

    @JsonProperty
    public List<BasicStageInfo> getStages()
    {
        return stages;
    }

    @JsonIgnore
    public Map<StageId, BasicStageInfo> getStagesById()
    {
        return stagesById;
    }

    @JsonIgnore
    public BasicStageInfo getOutputStage()
    {
        return stagesById.get(outputStageId);
    }

    public static List<BasicStageInfo> getAllStages(Optional<BasicStagesInfo> stages)
    {
        return stages.map(BasicStagesInfo::getStages).orElse(ImmutableList.of());
    }
}
