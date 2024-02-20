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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record WriterScalingOptions(boolean isWriterTasksScalingEnabled, boolean isPerTaskWriterScalingEnabled, Optional<Integer> perTaskMaxScaledWriterCount)
{
    public static final WriterScalingOptions DISABLED = new WriterScalingOptions(false, false);
    public static final WriterScalingOptions ENABLED = new WriterScalingOptions(true, true);

    public WriterScalingOptions(boolean writerTasksScalingEnabled, boolean perTaskWriterScalingEnabled)
    {
        this(writerTasksScalingEnabled, perTaskWriterScalingEnabled, Optional.empty());
    }

    @JsonCreator
    public WriterScalingOptions(
            @JsonProperty boolean isWriterTasksScalingEnabled,
            @JsonProperty boolean isPerTaskWriterScalingEnabled,
            @JsonProperty Optional<Integer> perTaskMaxScaledWriterCount)
    {
        this.isWriterTasksScalingEnabled = isWriterTasksScalingEnabled;
        this.isPerTaskWriterScalingEnabled = isPerTaskWriterScalingEnabled;
        this.perTaskMaxScaledWriterCount = requireNonNull(perTaskMaxScaledWriterCount, "perTaskMaxScaledWriterCount is null");
    }

    @Override
    @JsonProperty
    public boolean isWriterTasksScalingEnabled()
    {
        return isWriterTasksScalingEnabled;
    }

    @Override
    @JsonProperty
    public boolean isPerTaskWriterScalingEnabled()
    {
        return isPerTaskWriterScalingEnabled;
    }

    @JsonProperty
    public Optional<Integer> perTaskMaxScaledWriterCount()
    {
        return perTaskMaxScaledWriterCount;
    }
}
