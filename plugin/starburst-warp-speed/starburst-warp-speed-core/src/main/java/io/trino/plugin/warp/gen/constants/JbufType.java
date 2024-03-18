
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

public enum JbufType
{
    JBUF_TYPE_LUCENE_SMALL_SI,
    JBUF_TYPE_LUCENE_SMALL_CFE,
    JBUF_TYPE_LUCENE_SMALL_SEGMENTS,
    JBUF_TYPE_LUCENE_BIG_CFS,
    JBUF_TYPE_LUCENE_MATCH_BM,
    JBUF_TYPE_REC,
    JBUF_TYPE_NULL,
    JBUF_TYPE_QUERY_NUM_OF,
    JBUF_TYPE_CHUNKS_MAP,
    JBUF_TYPE_CRC,
    JBUF_TYPE_SKIPLIST,
    JBUF_TYPE_EXT_RECS,
    JBUF_TYPE_TEMP,
    JBUF_TYPE_NUM_OF;

    JbufType()
    {
    }
}
