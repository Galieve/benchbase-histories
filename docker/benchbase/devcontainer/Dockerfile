# For the devcontainer we need a full JDK.
#FROM --platform=linux maven:3.8.5-eclipse-temurin-17 AS devcontainer
FROM maven:3.9.3-eclipse-temurin-17 AS devcontainer

LABEL org.opencontainers.image.source = "https://github.com/cmu-db/benchbase/"

# Make sure the image is patched and up to date.
# Also add a few nice cli tools.
RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install --no-install-recommends sudo vim-nox neovim less bash-completion colordiff git openssh-client jq \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add a containeruser that allows vscode/codespaces to map the local host user
# (often uid 1000) to some non-root user inside the container.
ARG CONTAINERUSER_UID=1000
ARG CONTAINERUSER_GID=1000
RUN groupadd --non-unique --gid ${CONTAINERUSER_GID} containergroup \
    && useradd --non-unique --create-home --no-user-group --comment 'Container User' \
        --uid ${CONTAINERUSER_UID} --gid ${CONTAINERUSER_GID} containeruser \
    && echo 'containeruser ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN mkdir -p /benchbase/results && chown -R containeruser:containergroup /benchbase/

RUN apt-get update && apt-get -y install bash

RUN apt-get update && apt-get -y install xmlstarlet

RUN apt-get update && apt-get -y upgrade && apt-get -y install python3-pip && apt-get -y install python3-venv

RUN apt-get -y install python3-pandas python3-numpy python3-matplotlib python3-seaborn

RUN python3 -m venv .venv

USER containeruser
# Unset the upstream MVN_CONFIG value - it interacts poorly with the mvnw wrapper.
ENV MAVEN_CONFIG=
ENV MAVEN_CONFIG_DIR=/home/containeruser/.m2
WORKDIR /benchbase
VOLUME /benchbase/results

# At the expense of having to download some of the maven project dependencies
# at build time (many others we'd have to get for building the different
# profiles anyways), but for the benefit of not having to rebuild the
# devcontainer as frequently, we set the context to /dev/null during build and
# avoid copying any content over now.
#
## Assumes the context is given as the root of the repo.
#
## Preload some dependencies.
#COPY --chown=containeruser:containergroup pom.xml /benchbase/pom.xml
#COPY --chown=containeruser:containergroup .git/ /benchbase/.git/
#ARG MAVEN_OPTS
#RUN mvn -T2C -B --file pom.xml initialize \
#    && rm -rf /benchbase/.git /benchbase/target /benchbase/pom.xml

# When running the devcontainer, just launch an interactive shell by default.
ENV SHELL=/bin/bash
ENTRYPOINT ["/bin/bash", "-l"]
