
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

public enum WarmUpType
{
    WARM_UP_TYPE_BASIC(false),
    WARM_UP_TYPE_LUCENE(false),
    WARM_UP_TYPE_DATA(false),
    WARM_UP_TYPE_BLOOM_HIGH(true),
    WARM_UP_TYPE_BLOOM_MEDIUM(true),
    WARM_UP_TYPE_BLOOM_LOW(true),
    WARM_UP_TYPE_NUM_OF(false);
    final boolean bloom;

    WarmUpType(boolean bloom)
    {
        this.bloom = bloom;
    }

    public boolean bloom()
    {
        return bloom;
    }
}
