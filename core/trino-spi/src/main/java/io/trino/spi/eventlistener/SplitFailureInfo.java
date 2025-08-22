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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
@Deprecated(forRemoval = true)
public class SplitFailureInfo
{
    private final String failureType;
    private final String failureMessage;

    @JsonCreator
    @Unstable
    public SplitFailureInfo(String failureType, String failureMessage)
    {
        this.failureType = requireNonNull(failureType, "failureType is null");
        this.failureMessage = requireNonNull(failureMessage, "failureMessage is null");
    }

    @JsonProperty
    public String getFailureType()
    {
        return failureType;
    }

    @JsonProperty
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
