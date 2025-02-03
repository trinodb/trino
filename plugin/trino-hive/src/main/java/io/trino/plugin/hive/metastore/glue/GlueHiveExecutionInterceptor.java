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
package io.trino.plugin.hive.metastore.glue;

import com.google.inject.Inject;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

public class GlueHiveExecutionInterceptor
        implements ExecutionInterceptor
{
    private final boolean skipArchive;

    @Inject
    GlueHiveExecutionInterceptor(GlueHiveMetastoreConfig config)
    {
        this.skipArchive = config.isSkipArchive();
    }

    @Override
    public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes)
    {
        if (context.request() instanceof UpdateTableRequest updateTableRequest) {
            return updateTableRequest.toBuilder().skipArchive(skipArchive).build();
        }
        return context.request();
    }
}
