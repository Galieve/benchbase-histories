# TODO: Use a multi-stage build to build the fullimage from the devcontainer.

# Use a smaller base image that only has the jre, not the full jdk.
FROM eclipse-temurin:23-jre AS fullimage

LABEL org.opencontainers.image.source = "https://github.com/cmu-db/benchbase/"

# Make sure the image is patched and up to date.
RUN apt update && apt -y upgrade && apt clean && rm -rf /var/lib/apt/lists/*

# Add a containeruser that allows vscode/codespaces to map the local host user
# (often uid 1000) to some non-root user inside the container.
ARG CONTAINERUSER_UID=1000
ARG CONTAINERUSER_GID=1000
RUN groupadd --non-unique --gid ${CONTAINERUSER_GID} containergroup \
    && useradd --non-unique --create-home --no-user-group --comment 'Container User' \
        --uid ${CONTAINERUSER_UID} --gid ${CONTAINERUSER_GID} containeruser
RUN mkdir -p /benchbase/results && chown -R containeruser:containergroup /benchbase/results

RUN apt-get update && apt-get -y install bash

RUN apt-get update && apt-get -y install xmlstarlet

RUN apt-get update && apt-get -y upgrade && apt-get -y install python3-pip python3-venv

RUN apt-get -y install python3-pandas python3-numpy python3-seaborn python3-matplotlib

RUN apt-get -y install texlive texlive-latex-extra texlive-fonts-recommended cm-super dvipng

#RUN apt-get -y install texlive texlive-latex-extra texlive-fonts-recommended cm-super dvipng
RUN python3 -m venv .venv

RUN . .venv/bin/activate

RUN .venv/bin/pip3 install pandas seaborn matplotlib


RUN wget -qO - https://download.sublimetext.com/sublimehq-pub.gpg | apt-key add - && \
    apt-get install apt-transport-https && \
    echo "deb https://download.sublimetext.com/ apt/stable/" | tee /etc/apt/sources.list.d/sublime-text.list && \
    apt-get update && \
    apt-get install sublime-text


USER containeruser
WORKDIR /benchbase
VOLUME /benchbase/results


# Only copy the content necessary for running (not building) benchbase over to the image.
# These files should have been built using the devcontainer by the
# build-full-image.sh script.

COPY --chown=containeruser:containergroup . /benchbase/

ENV SHELL=/bin/bash
ENTRYPOINT ["/bin/bash", "-l"]

#ENTRYPOINT ["/benchbase/entrypoint.sh"]
#CMD ["--help"]