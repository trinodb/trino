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
package io.prestosql.plugin.kinesis;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.prestosql.decoder.DecoderModule;
import io.prestosql.plugin.kinesis.s3config.S3TableConfigClient;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

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

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
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

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(TypeId.of(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
