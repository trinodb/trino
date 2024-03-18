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

import io.trino.spi.cache.PlanSignature;

public class WarpCacheData
{
    private final PlanSignature planSignature;
    private final long signatureId;
    private final long[] columnIds;

    public WarpCacheData(PlanSignature planSignature, long signatureId, long[] columnIds)
    {
        this.planSignature = planSignature;
        this.signatureId = signatureId;
        this.columnIds = columnIds;
    }

    public PlanSignature getPlanSignature()
    {
        return planSignature;
    }

    public long getSignatureId()
    {
        return signatureId;
    }

    public long[] getColumnIds()
    {
        return columnIds;
    }
}
