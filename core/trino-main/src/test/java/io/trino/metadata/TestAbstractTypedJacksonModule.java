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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.json.JsonMapperProvider;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.BeanDescription;
import tools.jackson.databind.SerializationConfig;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanPropertyWriter;
import tools.jackson.databind.ser.ValueSerializerModifier;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAbstractTypedJacksonModule
{
    @Test
    public void testSerializerModifiersApplyToSubtypes()
    {
        ListMultimap<Class<?>, String> modifiers = ArrayListMultimap.create();
        JsonMapper mapper = new JsonMapperProvider()
                .withModules(ImmutableSet.of(recordingModule("recording", modifiers), new TestHandleModule()))
                .get();

        TestHandle handle = new TestHandle("table");
        String json = mapper.writeValueAsString(handle);

        assertThat(modifiers.get(TestHandle.class)).containsExactly("recording");
        assertThat(json).contains("\"@type\":\"test\"");
        assertThat(mapper.readValue(json, Handle.class)).isEqualTo(handle);
    }

    @Test
    public void testTypeIdOnlyWrittenForSubtypes()
    {
        JsonMapper mapper = new JsonMapperProvider()
                .withModules(ImmutableSet.of(new TestHandleModule()))
                .get();

        assertThat(mapper.writeValueAsString(new TestHandle("table"))).isEqualTo("{\"@type\":\"test\",\"name\":\"table\"}");
        assertThat(mapper.writeValueAsString(new TestBean("bean"))).isEqualTo("{\"name\":\"bean\"}");
    }

    @Test
    public void testSerializerModifiersKeepMapperOrder()
    {
        ListMultimap<Class<?>, String> modifiers = ArrayListMultimap.create();
        JsonMapper mapper = new JsonMapperProvider()
                .withModules(ImmutableSet.of(recordingModule("first", modifiers), recordingModule("second", modifiers), new TestHandleModule()))
                .get();

        mapper.writeValueAsString(new TestBean("bean"));
        mapper.writeValueAsString(new TestHandle("table"));

        assertThat(modifiers.get(TestBean.class)).containsExactlyInAnyOrder("first", "second");
        assertThat(modifiers.get(TestHandle.class)).isEqualTo(modifiers.get(TestBean.class));
    }

    @Test
    public void testSerializerModifiersPerMapper()
    {
        ListMultimap<Class<?>, String> modifiers = ArrayListMultimap.create();
        TestHandleModule handleModule = new TestHandleModule();
        JsonMapper left = new JsonMapperProvider()
                .withModules(ImmutableSet.of(recordingModule("left", modifiers), handleModule))
                .get();
        JsonMapper right = new JsonMapperProvider()
                .withModules(ImmutableSet.of(recordingModule("right", modifiers), handleModule))
                .get();

        left.writeValueAsString(new TestHandle("table"));
        right.writeValueAsString(new TestHandle("table"));

        assertThat(modifiers.get(TestHandle.class)).containsExactly("left", "right");
    }

    private static SimpleModule recordingModule(String name, ListMultimap<Class<?>, String> modifiers)
    {
        return new SimpleModule(name).setSerializerModifier(new ValueSerializerModifier()
        {
            @Override
            public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription.Supplier beanDescription, List<BeanPropertyWriter> beanProperties)
            {
                modifiers.put(beanDescription.getBeanClass(), name);
                return beanProperties;
            }
        });
    }

    public record TestBean(String name) {}

    public interface Handle {}

    public static class TestHandle
            implements Handle
    {
        private final String name;

        @JsonCreator
        public TestHandle(@JsonProperty("name") String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof TestHandle that && name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }
    }

    private static class TestHandleModule
            extends AbstractTypedJacksonModule<Handle>
    {
        public TestHandleModule()
        {
            super(Handle.class, _ -> "test", _ -> TestHandle.class);
        }
    }
}
