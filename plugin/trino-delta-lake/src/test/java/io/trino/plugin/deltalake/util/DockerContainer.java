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
package io.trino.plugin.deltalake.util;

import org.testcontainers.containers.FixedHostPortGenericContainer;

// copied from an earlier version of io.trino.tests.product.launcher.env.DockerContainer
class DockerContainer
        extends FixedHostPortGenericContainer<DockerContainer>
{
    public DockerContainer(String dockerImageName)
    {
        super(dockerImageName);
    }
}
