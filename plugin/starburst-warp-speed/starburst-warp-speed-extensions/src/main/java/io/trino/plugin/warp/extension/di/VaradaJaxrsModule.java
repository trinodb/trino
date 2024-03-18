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
package io.trino.plugin.warp.extension.di;

import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;
import io.airlift.http.server.TheServlet;
import io.airlift.jaxrs.JaxrsBinder;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jaxrs.JaxrsResource;
import io.airlift.jaxrs.JsonMapper;
import io.airlift.jaxrs.ParsingExceptionMapper;
import io.airlift.jaxrs.SmileMapper;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.trino.plugin.varada.util.Auditer;
import io.trino.plugin.varada.util.TrinoExceptionMapper;
import io.trino.plugin.warp.extension.execution.CorsFilter;
import io.trino.plugin.warp.extension.execution.VaradaExtResource;
import io.varada.annotation.Audit;
import jakarta.servlet.Servlet;
import org.glassfish.jersey.servlet.ServletContainer;

import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class VaradaJaxrsModule
        extends JaxrsModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.disableCircularProxies();

        binder.bind(Servlet.class).annotatedWith(TheServlet.class).to(Key.get(ServletContainer.class));
        ObjectMapper objectMapper = new ObjectMapper();
        // ignore unknown fields (for backwards compatibility)
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // do not allow converting a float to an integer
        objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);

        // use ISO dates
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        final DeserializationConfig newDeserializationConfig = objectMapper.getDeserializationConfig().with(MapperFeature.AUTO_DETECT_CREATORS)
                .with(MapperFeature.AUTO_DETECT_FIELDS)
                .with(MapperFeature.AUTO_DETECT_SETTERS)
                .with(MapperFeature.AUTO_DETECT_GETTERS)
                .with(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .with(MapperFeature.USE_GETTERS_AS_SETTERS)
                .with(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .with(MapperFeature.INFER_PROPERTY_MUTATORS)
                .with(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

        final SerializationConfig newSerializationConfig = objectMapper.getSerializationConfig().with(MapperFeature.AUTO_DETECT_CREATORS)
                .with(MapperFeature.AUTO_DETECT_FIELDS)
                .with(MapperFeature.AUTO_DETECT_SETTERS)
                .with(MapperFeature.AUTO_DETECT_GETTERS)
                .with(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .with(MapperFeature.USE_GETTERS_AS_SETTERS)
                .with(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .with(MapperFeature.INFER_PROPERTY_MUTATORS)
                .with(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);
        objectMapper.setConfig(newSerializationConfig);
        objectMapper.setConfig(newDeserializationConfig);

        objectMapper.registerModules(new JavaTimeModule(), new Jdk8Module(), new JodaModule(), new ParameterNamesModule(), new GuavaModule());
        JsonMapper mapper = new JsonMapper(objectMapper);
        JaxrsBinder.jaxrsBinder(binder).bindInstance(mapper);
        JaxrsBinder.jaxrsBinder(binder).bind(SmileMapper.class);
        JaxrsBinder.jaxrsBinder(binder).bind(ParsingExceptionMapper.class);

        Multibinder.newSetBinder(binder, Object.class, JaxrsResource.class).permitDuplicates();

        jaxrsBinder(binder).bind(VaradaExtResource.class);
        jaxrsBinder(binder).bind(OpenApiResource.class);
        jaxrsBinder(binder).bind(TrinoExceptionMapper.class);
        jaxrsBinder(binder).bind(CorsFilter.class);
        httpServerBinder(binder).bindResource("/swagger", "webapp/ui").withWelcomeFile("index.html");
//        httpServerBinder(binder).bindResource("swagger.json", "webapp").withWelcomeFile("swagger.json");
        httpServerBinder(binder).bindResource("openapi.json", "webapp").withWelcomeFile("openapi.json");
        binder.bindInterceptor(Matchers.any(), Matchers.annotatedWith(Audit.class), new Auditer());
    }
}
