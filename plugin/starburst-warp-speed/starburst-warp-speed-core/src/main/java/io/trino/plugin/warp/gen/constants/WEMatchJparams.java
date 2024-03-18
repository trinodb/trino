
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

public enum WEMatchJparams
{
    WE_MATCH_JPARAMS_FILE_OFFSET,
    WE_MATCH_JPARAMS_FILE_READ_SIZE,
    WE_MATCH_JPARAMS_WARM_UP_TYPE,
    WE_MATCH_JPARAMS_REC_TYPE_CODE,
    WE_MATCH_JPARAMS_REC_TYPE_LENGTH,
    WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_HIGH,
    WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_LOW,
    WE_MATCH_JPARAMS_IS_COLLECT_NULLS,
    WE_MATCH_JPARAMS_IS_TIGHTNESS_REQUIRED,
    WE_MATCH_JPARAMS_MATCH_COLLECT_INDEX,
    WE_MATCH_JPARAMS_MATCH_COLLECT_OP,
    WE_MATCH_JPARAMS_NUM_OF;

    WEMatchJparams()
    {
    }
}
