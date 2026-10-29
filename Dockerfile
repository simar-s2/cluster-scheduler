# One Dockerfile, two images. Build the control plane image with
# "--target control-plane" and the worker image with "--target worker".
# Docker Compose picks the target for each service.

# Stage 1: compile both applications with the Gradle wrapper.
FROM eclipse-temurin:21.0.13_9-jdk-noble AS build
WORKDIR /source
COPY . .
# The cache mount keeps Gradle's downloads between builds.
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon --quiet :control-plane:installDist :worker:installDist

# Stage 2a: the control plane, with only a JRE and the built application.
FROM eclipse-temurin:21.0.13_9-jre-noble AS control-plane
COPY --from=build /source/control-plane/build/install/control-plane /app
ENTRYPOINT ["/app/bin/control-plane"]

# Stage 2b: the worker.
FROM eclipse-temurin:21.0.13_9-jre-noble AS worker
COPY --from=build /source/worker/build/install/worker /app
ENTRYPOINT ["/app/bin/worker"]
