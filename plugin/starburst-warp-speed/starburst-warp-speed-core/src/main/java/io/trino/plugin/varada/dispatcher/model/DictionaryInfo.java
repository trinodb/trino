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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public record DictionaryInfo(
        DictionaryKey dictionaryKey,
        DictionaryState dictionaryState,
        int dataValuesRecTypeLength,
        int dictionaryOffset)
        implements Serializable
{
    public static final int NO_OFFSET = -1;

    @JsonCreator
    public DictionaryInfo(
            @JsonProperty("dictionaryKey") DictionaryKey dictionaryKey,
            @JsonProperty("dictionaryState") DictionaryState dictionaryState,
            @JsonProperty("dataValuesRecTypeLength") int dataValuesRecTypeLength,
            @JsonProperty("dictionaryOffset") int dictionaryOffset)
    {
        this.dictionaryKey = dictionaryKey;
        this.dictionaryState = dictionaryState;
        this.dataValuesRecTypeLength = dataValuesRecTypeLength;
        this.dictionaryOffset = dictionaryOffset;
    }
}
