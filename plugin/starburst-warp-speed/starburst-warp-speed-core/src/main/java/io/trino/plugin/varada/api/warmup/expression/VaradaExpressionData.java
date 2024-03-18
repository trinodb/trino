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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = VaradaExpressionData.CLASS_TYPE)
@JsonSubTypes({
        @JsonSubTypes.Type(value = VaradaPrimitiveConstantData.class, name = "VaradaPrimitiveConstant")
})
public interface VaradaExpressionData
{
    String CLASS_TYPE = "classType";
    String TYPE = "type";

    @JsonProperty(TYPE)
    Type getType();

    enum Type {
        VARCHAR,
        INTEGER,
        BIGINT,
        SMALLINT,
        DOUBLE,
        REAL
    }
}
