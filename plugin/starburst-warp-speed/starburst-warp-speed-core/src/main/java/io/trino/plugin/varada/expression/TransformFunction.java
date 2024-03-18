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
package io.trino.plugin.varada.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public record TransformFunction(
        @JsonProperty("transformType") io.trino.plugin.varada.expression.TransformFunction.TransformType transformType,
        @JsonProperty("transformParams") List<? extends VaradaConstant> transformParams)
{
    public static final TransformFunction NONE = new TransformFunction(TransformType.NONE);
    public static final TransformFunction LOWER = new TransformFunction(TransformType.LOWER);
    public static final TransformFunction UPPER = new TransformFunction(TransformType.UPPER);
    public static final TransformFunction DATE = new TransformFunction(TransformType.DATE);

    @JsonCreator
    public TransformFunction {}

    public TransformFunction(TransformType transformType)
    {
        this(transformType, Collections.emptyList());
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
