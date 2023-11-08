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
package io.trino.plugin.password.file;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.spi.security.GroupProvider;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;

public class TestFileGroupProvider
{
    @Test
    public void test()
            throws Exception
    {
        File groupFile = new File(Resources.getResource("group.txt").toURI().getPath());
        GroupProvider groupProvider = new FileGroupProvider(new FileGroupConfig().setGroupFile(groupFile));
        assertGroupProvider(groupProvider);
    }

    @Test
    public void testGroupProviderFactory()
            throws Exception
    {
        File groupFile = new File(Resources.getResource("group.txt").toURI().getPath());
        GroupProvider groupProvider = new FileGroupProviderFactory()
                .create(ImmutableMap.of("file.group-file", groupFile.getAbsolutePath()));
        assertGroupProvider(groupProvider);
    }

    private static void assertGroupProvider(GroupProvider groupProvider)
    {
        assertEquals(groupProvider.getGroups("user_1"), ImmutableSet.of("group_a", "group_b"));
        assertEquals(groupProvider.getGroups("user_2"), ImmutableSet.of("group_a"));
        assertEquals(groupProvider.getGroups("user_3"), ImmutableSet.of("group_b"));
        assertEquals(groupProvider.getGroups("unknown"), ImmutableSet.of());
    }
}
