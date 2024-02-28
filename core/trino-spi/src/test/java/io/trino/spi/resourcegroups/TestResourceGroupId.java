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
package io.trino.spi.resourcegroups;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceGroupId
{
    @Test
    public void testBasic()
    {
        new ResourceGroupId("test_test");
        new ResourceGroupId("test.test");
        new ResourceGroupId(new ResourceGroupId("test"), "test");
    }

    @Test
    public void testCodec()
    {
        JsonCodec<ResourceGroupId> codec = JsonCodec.jsonCodec(ResourceGroupId.class);
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("test.test"), "foo");
        assertThat(codec.fromJson(codec.toJson(resourceGroupId))).isEqualTo(resourceGroupId);

        assertThat(codec.toJson(resourceGroupId)).isEqualTo("[ \"test.test\", \"foo\" ]");
        assertThat(codec.fromJson("[\"test.test\", \"foo\"]")).isEqualTo(resourceGroupId);
    }

    @Test
    public void testIsAncestor()
    {
        ResourceGroupId root = new ResourceGroupId("root");
        ResourceGroupId rootA = new ResourceGroupId(root, "a");
        ResourceGroupId rootAFoo = new ResourceGroupId(rootA, "foo");
        ResourceGroupId rootBar = new ResourceGroupId(root, "bar");
        assertThat(root.isAncestorOf(rootA)).isTrue();
        assertThat(root.isAncestorOf(rootAFoo)).isTrue();
        assertThat(root.isAncestorOf(rootBar)).isTrue();
        assertThat(rootA.isAncestorOf(rootAFoo)).isTrue();
        assertThat(rootA.isAncestorOf(rootBar)).isFalse();
        assertThat(rootAFoo.isAncestorOf(rootBar)).isFalse();
        assertThat(rootBar.isAncestorOf(rootAFoo)).isFalse();
        assertThat(rootAFoo.isAncestorOf(root)).isFalse();
        assertThat(root.isAncestorOf(root)).isFalse();
        assertThat(rootAFoo.isAncestorOf(rootAFoo)).isFalse();
    }
}
