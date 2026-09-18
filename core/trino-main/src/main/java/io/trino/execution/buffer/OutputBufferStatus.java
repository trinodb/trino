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
package io.trino.execution.buffer;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record OutputBufferStatus(
        OptionalLong outputBuffersVersion,
        boolean overutilized,
        boolean exchangeSinkInstanceHandleUpdateRequired)
{
    private static final OutputBufferStatus INITIAL = new OutputBufferStatus(OptionalLong.empty(), false, false);

    public OutputBufferStatus
    {
        requireNonNull(outputBuffersVersion, "outputBuffersVersion is null");
    }

    public static OutputBufferStatus initial()
    {
        return INITIAL;
    }

    public static Builder builder(long outputBuffersVersion)
    {
        return new Builder(outputBuffersVersion);
    }

    public static class Builder
    {
        private final OptionalLong outputBuffersVersion;
        private boolean overutilized;
        private boolean exchangeSinkInstanceHandleUpdateRequired;

        public Builder(long outputBuffersVersion)
        {
            this.outputBuffersVersion = OptionalLong.of(outputBuffersVersion);
        }

        public Builder setOverutilized(boolean overutilized)
        {
            this.overutilized = overutilized;
            return this;
        }

        public Builder setExchangeSinkInstanceHandleUpdateRequired(boolean exchangeSinkInstanceHandleUpdateRequired)
        {
            this.exchangeSinkInstanceHandleUpdateRequired = exchangeSinkInstanceHandleUpdateRequired;
            return this;
        }

        public OutputBufferStatus build()
        {
            return new OutputBufferStatus(outputBuffersVersion, overutilized, exchangeSinkInstanceHandleUpdateRequired);
        }
    }
}
