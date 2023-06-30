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
package io.trino.plugin.hudi.model;

/**
 * Type of the Hoodie Table.
 * <p>
 * Currently, 2 types are supported.
 * <ul>
 * <li> COPY_ON_WRITE - Performs upserts by versioning entire files, with later versions containing newer value of a record.
 * <li> MERGE_ON_READ - Speeds up upserts, by delaying merge until enough work piles up.
 * </ul>
 */
public enum HudiTableType
{
    COPY_ON_WRITE,
    MERGE_ON_READ
}
