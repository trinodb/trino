/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.cicd;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEcrPullThroughNameSubstitutor
{
    @Test
    public void testSubstitutions()
    {
        // Official library
        assertImageName("ecr-registry.com/docker-hub/", "mysql:8.15", "ecr-registry.com/docker-hub/library/mysql:8.15");
        // Rewrite to ECR
        assertImageName("ecr-registry.com/docker-hub/", "repository/mysql:8.15", "ecr-registry.com/docker-hub/repository/mysql:8.15");
        // No rewrite - custom registry
        assertImageName("ecr-registry.com/docker-hub/", "registry.com/repository/mysql:8.15", "registry.com/repository/mysql:8.15");
        // No rewrite - no target registry
        assertImageName("", "mysql:8.15", "mysql:8.15");
    }

    private void assertImageName(String registry, String imageName, String expectedImageName)
    {
        assertThat(new EcrPullThroughNameSubstitutor(registry).apply(DockerImageName.parse(imageName)))
                .isEqualTo(DockerImageName.parse(expectedImageName));
    }
}
