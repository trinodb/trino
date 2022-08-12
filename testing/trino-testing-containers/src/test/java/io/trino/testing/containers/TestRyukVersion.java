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
package io.trino.testing.containers;

import com.github.dockerjava.api.model.Container;
import com.google.common.io.Resources;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.ResourceReaper;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRyukVersion
{
    @Test
    public void testRyukVersionResourceFile()
            throws Exception
    {
        ResourceReaper.instance();
        List<Container> containers = DockerClientFactory.instance().client()
                .listContainersCmd().withNameFilter(List.of("testcontainers-ryuk-" + DockerClientFactory.SESSION_ID))
                .exec();
        assertThat(containers).as("containers")
                .hasSize(1);

        String currentImage = getOnlyElement(containers).getImage();
        // Sanity check the value we get from testcontainers includes version
        assertThat(currentImage).as("currentImage")
                .matches("testcontainers/ryuk:(.+)")
                .doesNotContain("latest");

        String ryukImageFromResource = Resources.toString(Resources.getResource(getClass(), "testcontainers-ryuk-currentImage.txt"), UTF_8).trim();
        assertThat(ryukImageFromResource).as("ryukImageFromResource")
                .isEqualTo(currentImage);
    }
}
