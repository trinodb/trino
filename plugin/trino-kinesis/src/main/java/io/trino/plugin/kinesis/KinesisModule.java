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
package io.trino.plugin.kinesis;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.kinesis.s3config.S3TableConfigClient;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class KinesisModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Note: handle resolver handled separately, along with several other classes.
        binder.bind(KinesisConnector.class).in(Scopes.SINGLETON);

        binder.bind(KinesisMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KinesisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KinesisRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(S3TableConfigClient.class).in(Scopes.SINGLETON);
        binder.bind(KinesisSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(KinesisConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(KinesisStreamDescription.class);

        binder.install(new DecoderModule());

        for (KinesisInternalFieldDescription internalFieldDescription : KinesisInternalFieldDescription.values()) {
            bindInternalColumn(binder, internalFieldDescription);
        }
    }

    private static void bindInternalColumn(Binder binder, KinesisInternalFieldDescription fieldDescription)
    {
        Multibinder<KinesisInternalFieldDescription> fieldDescriptionBinder = Multibinder.newSetBinder(binder, KinesisInternalFieldDescription.class);
        fieldDescriptionBinder.addBinding().toInstance(fieldDescription);
    }
}
