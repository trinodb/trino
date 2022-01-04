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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.ErrorLocation;
import io.trino.client.FailureInfo;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.NodeVersion;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.RowFieldName;
import io.trino.client.ServerInfo;
import io.trino.client.StageStats;
import io.trino.client.StatementStats;
import io.trino.client.Warning;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

import java.util.List;

@SuppressWarnings("unused")
public class RuntimeReflectionRegistrationFeature implements Feature
{
    public void beforeAnalysis(BeforeAnalysisAccess access) {
        List<Class<?>> classes = ImmutableList.of(
                Column.class,
                QueryError.class,
                QueryResults.class,
                StatementStats.class,
                Warning.class,
                StageStats.class,
                ErrorLocation.class,
                FailureInfo.class,
                ServerInfo.class,
                NamedClientTypeSignature.class,
                NodeVersion.class,
                ClientSelectedRole.class,
                ClientTypeSignature.class,
                RowFieldName.class,
                ClientTypeSignatureParameter.class,
                ClientTypeSignatureParameter.ClientTypeSignatureParameterDeserializer.class);

        classes.forEach(clazz -> {
            RuntimeReflection.register(clazz);
            RuntimeReflection.register(clazz.getDeclaredConstructors());
            RuntimeReflection.register(clazz.getConstructors());
            RuntimeReflection.register(clazz.getDeclaredMethods());
            RuntimeReflection.register(clazz.getMethods());
            RuntimeReflection.register(clazz.getDeclaredClasses());
            RuntimeReflection.register(clazz.getClasses());
        });
    }
}
