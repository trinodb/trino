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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = VaradaColumn.CLASS_TYPE)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RegularColumn.class, name = "RegularColumn"),
        @JsonSubTypes.Type(value = WildcardColumn.class, name = "WildcardColumn"),
        @JsonSubTypes.Type(value = TransformedColumn.class, name = "TransformedColumn")
})
public interface VaradaColumn
        extends Serializable
{
    String CLASS_TYPE = "classType";
    String COLUMN_NAME = "name";
    String COLUMN_ID = "columnId";

    @JsonProperty(COLUMN_NAME)
    String getName();

    @JsonProperty(COLUMN_ID)
    String getColumnId();

    @JsonIgnore
    // small is the least important
    int getOrder();

    @JsonIgnore
    default boolean isTransformedColumn()
    {
        return false;
    }
}
