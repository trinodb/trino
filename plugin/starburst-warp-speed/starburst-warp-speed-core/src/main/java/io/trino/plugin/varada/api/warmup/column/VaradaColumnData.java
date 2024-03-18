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
package io.trino.plugin.varada.api.warmup.column;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = VaradaColumnData.CLASS_TYPE)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RegularColumnData.class, name = "RegularColumn"),
        @JsonSubTypes.Type(value = WildcardColumnData.class, name = "WildcardColumn"),
        @JsonSubTypes.Type(value = TransformedColumnData.class, name = "TransformedColumn")
})
public interface VaradaColumnData
{
    String CLASS_TYPE = "classType";
    String KEY = "key";

    @JsonProperty(KEY)
    String getKey();

    boolean contains(VaradaColumnData varadaColumnData);
}
