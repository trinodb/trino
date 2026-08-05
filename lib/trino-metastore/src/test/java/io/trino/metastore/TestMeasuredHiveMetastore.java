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
package io.trino.metastore;

import io.trino.metastore.MeasuredHiveMetastore.MeasuredMetastoreFactory;
import org.junit.jupiter.api.Test;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

class TestMeasuredHiveMetastore
{
    @Test
    public void testAllMethodsImplemented()
    {
        assertAllMethodsOverridden(HiveMetastore.class, MeasuredHiveMetastore.class);
    }

    @Test
    public void testAllFactoryMethodsImplemented()
    {
        assertAllMethodsOverridden(HiveMetastoreFactory.class, MeasuredMetastoreFactory.class);
    }
}
