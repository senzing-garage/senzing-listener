ARG BASE_IMAGE=senzing/senzingapi-runtime:3.7.1
ARG BASE_BUILDER_IMAGE=senzing/base-image-debian:1.0.20

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_BUILDER_IMAGE} as builder

ENV REFRESHED_AT=2023-09-29

LABEL Name="senzing/senzing-listener-builder" \
      Maintainer="support@senzing.com" \
      Version="0.5.1"

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Build "senzing-listener.jar".

COPY . /senzing-listener
WORKDIR /senzing-listener

RUN export SENZING_LISTENER_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
 && make package \
 && cp /senzing-listener/target/senzing-listener-${SENZING_LISTENER_VERSION}.jar "/senzing-listener.jar"

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2023-09-29

LABEL Name="senzing/senzing-listener" \
      Maintainer="support@senzing.com" \
      Version="0.5.1"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      software-properties-common \
 && rm -rf /var/lib/apt/lists/*

# Install Java-11.

RUN mkdir -p /etc/apt/keyrings \
 && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public > /etc/apt/keyrings/adoptium.asc

RUN echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" >> /etc/apt/sources.list

RUN apt update \
 && apt install -y temurin-11-jdk \
 && rm -rf /var/lib/apt/lists/*

# Copy files from repository.

COPY ./rootfs /

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-listener.jar" "/app/senzing-listener.jar"

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java", "-jar", "senzing-listener.jar"]
