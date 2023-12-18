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

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.System.getenv;

public class EcrPullThroughNameSubstitutor
        extends ImageNameSubstitutor
{
    private static final String ENV_KEY_NAME = "TESTCONTAINER_DOCKER_PULL_THROUGH_REGISTRY";
    private final String registry;

    public EcrPullThroughNameSubstitutor()
    {
        this(getenv(ENV_KEY_NAME));
    }

    EcrPullThroughNameSubstitutor(String registry)
    {
        this.registry = registry;
    }

    @Override
    public DockerImageName apply(DockerImageName dockerImageName)
    {
        // Already belongs to some registry
        if (!isNullOrEmpty(dockerImageName.getRegistry())) {
            return dockerImageName;
        }

        if (isNullOrEmpty(registry)) {
            return dockerImageName;
        }

        String repository = dockerImageName.getRepository();
        if (!repository.contains("/")) {
            repository = "library/" + repository;
        }

        return DockerImageName
                .parse(registry + repository + ":" + dockerImageName.getVersionPart())
                .asCompatibleSubstituteFor(dockerImageName);
    }

    @Override
    protected String getDescription()
    {
        return "ECR pull-through cache for hub.docker.com";
    }
}
