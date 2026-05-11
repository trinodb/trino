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
package io.trino.json;

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

/// A single entry of a tree-form JSON object. Duplicate keys (allowed under
/// SQL:2023 §9.42 `WITHOUT UNIQUE KEYS`) appear as separate `JsonObjectMember`
/// instances at their original positions in the parent [JsonObject]'s member
/// list.
public record JsonObjectMember(Slice key, Json value)
{
    public JsonObjectMember
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
    }
}
