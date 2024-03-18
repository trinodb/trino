
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

package io.trino.plugin.warp.gen.constants;

public enum PredicateHeaderFlags
{
    PREDICATE_FLAG_HAS_FUNCTION(6),
    PREDICATE_FLAG_COLLECT_NULL(7);
    final int offset;

    PredicateHeaderFlags(int offset)
    {
        this.offset = offset;
    }

    public int offset()
    {
        return offset;
    }
}
