# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.177.0/containers/java/.devcontainer/base.Dockerfile

# [Choice] Java version: 11, 15
ARG VARIANT="17"
FROM mcr.microsoft.com/vscode/devcontainers/java:0-${VARIANT}

# [Option] Install zsh
ARG INSTALL_ZSH="true"
# [Option] Upgrade OS packages to their latest versions
ARG UPGRADE_PACKAGES="false"
# [Option] Enable non-root Docker access in container
ARG ENABLE_NONROOT_DOCKER="true"
# [Option] Use the OSS Moby CLI instead of the licensed Docker CLI
ARG USE_MOBY="true"

# Install needed packages and setup non-root user.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Copy custom CA certificates
COPY .work/*.crt /usr/local/share/ca-certificates/

COPY library-scripts/*.sh /tmp/library-scripts/
RUN update-ca-certificates \
    && apt-get update \
    && /bin/bash /tmp/library-scripts/common-debian.sh "${INSTALL_ZSH}" "${USERNAME}" "${USER_UID}" "${USER_GID}" "${UPGRADE_PACKAGES}" "true" "true" \
    # Use Docker script from script library to set things up
    && /bin/bash /tmp/library-scripts/docker-in-docker-debian.sh "${ENABLE_NONROOT_DOCKER}" "${USERNAME}" "${USE_MOBY}" \
    # Install the Azure CLI
    && bash /tmp/library-scripts/azcli-debian.sh \
    # Clean up
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/library-scripts/ \
    # Trust the GitHub public RSA key
    # This key was manually validated by running 'ssh-keygen -lf <key-file>' and comparing the fingerprint to the one found at:
    # https://docs.github.com/en/github/authenticating-to-github/githubs-ssh-key-fingerprints
    && mkdir -p /home/${USERNAME}/.ssh \
    && echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> /home/${USERNAME}/.ssh/known_hosts \
    && chown -R ${USERNAME} /home/${USERNAME}/.ssh \
    && touch /usr/local/share/bash_history \
    && chown ${USERNAME} /usr/local/share/bash_history

# Add custom CA certificates to Java truststore
RUN true \
    # Any command which returns non-zero exit code will cause this shell script to exit immediately:
    && set -e \
    # Activate debugging to show execution details: all commands will be printed before execution
    &&  set -x \
    # Figure out where java is installed
    && JAVA_DIR=$(readlink -f "$(dirname $(which java))/../") \
    && for cert in /usr/local/share/ca-certificates/*; do \
        openssl x509 -outform der -in "$cert" -out /tmp/certificate.der; \
        ${JAVA_DIR}/bin/keytool -import -alias "$cert" -keystore ${JAVA_DIR}/lib/security/cacerts -file /tmp/certificate.der -deststorepass changeit -noprompt; \
    done \
    && rm /tmp/certificate.der

# Setting the ENTRYPOINT to docker-init.sh will configure non-root access to
# the Docker socket if "overrideCommand": false is set in devcontainer.json.
# The script will also execute CMD if you need to alter startup behaviors.
ENTRYPOINT [ "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]

# Use the Maven cache from the host and persist Bash history
RUN mkdir -p /usr/local/share/m2 \
    && chown -R ${USER_UID}:${USER_GID} /usr/local/share/m2 \
    && ln -s /usr/local/share/m2 /home/${USERNAME}/.m2

ARG MAVEN_VERSION=""
ARG TRINO_VERSION="400"

# Install Maven
RUN mkdir -p /tmp \
    # Install additional OS packages.
    && apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends bash-completion vim maven \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* \
    # Install Trino CLI
    && wget https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar -P /usr/local/bin \
    && chmod +x /usr/local/bin/trino-cli-${TRINO_VERSION}-executable.jar \
    && ln -s /usr/local/bin/trino-cli-${TRINO_VERSION}-executable.jar /usr/local/bin/trino

ENV M2_HOME=/opt/maven
ENV MAVEN_HOME=/opt/maven
ENV PATH=${M2_HOME}/bin:${PATH}
