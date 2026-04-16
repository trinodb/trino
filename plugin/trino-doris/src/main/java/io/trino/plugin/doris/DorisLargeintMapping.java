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
package io.trino.plugin.doris;

public enum DorisLargeintMapping
{
    // Safe default for 128-bit Doris integers when Trino precision compatibility is unclear.
    VARCHAR,
    // Reserved for a later phase once end-to-end precision and predicate semantics are implemented.
    DECIMAL,
}
