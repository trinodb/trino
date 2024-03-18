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
package io.trino.plugin.varada.api.warmup.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public record TransformFunctionData(
        @JsonProperty("transformType") io.trino.plugin.varada.api.warmup.expression.TransformFunctionData.TransformType transformType,
        @JsonProperty("transformParams") ImmutableList<? extends VaradaConstantData> transformParams)
{
    @JsonCreator
    public TransformFunctionData {}

    public TransformFunctionData(TransformType transformType)
    {
        this(transformType, ImmutableList.of());
    }

    public enum TransformType
    {
        NONE,
        LOWER,
        UPPER,
        DATE,
        ELEMENT_AT,
        JSON_EXTRACT_SCALAR
    }
}
