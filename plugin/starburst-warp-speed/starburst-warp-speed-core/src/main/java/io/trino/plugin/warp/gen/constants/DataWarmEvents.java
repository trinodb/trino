
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

public enum DataWarmEvents
{
    DATA_WARM_EVENTS_COMPRESSION_LZ,
    DATA_WARM_EVENTS_COMPRESSION_LZ_HIGH,
    DATA_WARM_EVENTS_ENCODE_BIT_PACKING,
    DATA_WARM_EVENTS_ENCODE_BIT_PACKING_DELTA,
    DATA_WARM_EVENTS_SINGLE_CHUNK,
    DATA_WARM_EVENTS_PACKED_CHUNK,
    DATA_WARM_EVENTS_SINGLE_CHUNK_ROLLBACK,
    DATA_WARM_EVENTS_EXT_RECS,
    DATA_WARM_EVENTS_NUM_OF;

    DataWarmEvents()
    {
    }
}
