// Programs that run the whole system in Docker Compose, break it on purpose,
// and measure what happens. Each one writes its numbers to results/*.json.

plugins {
    java
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(project(":proto"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.postgresql)
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
    }
}

// One task per experiment, run from the repository root so docker compose
// finds docker-compose.yml and results land in results/.
fun registerExperiment(taskName: String, mainClassName: String, summary: String) {
    tasks.register<JavaExec>(taskName) {
        group = "experiments"
        description = summary
        classpath = sourceSets["main"].runtimeClasspath
        mainClass = mainClassName
        workingDir = rootDir
    }
}

registerExperiment(
    "failoverChaos",
    "clusterscheduler.experiments.FailoverChaosExperiment",
    "Kill and pause the leader 200 times while submitting jobs, then count double placements")

registerExperiment(
    "nodeFailure",
    "clusterscheduler.experiments.NodeFailureExperiment",
    "Kill a worker and measure the time until all its tasks run on other nodes")

registerExperiment(
    "packing",
    "clusterscheduler.experiments.PackingBenchmark",
    "Submit 10,000 jobs to 100 nodes and measure CPU utilization")
