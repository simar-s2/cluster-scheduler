// A worker node: registers with the control plane, sends heartbeats, and runs
// the simulated tasks the leader assigns to it. It never touches PostgreSQL.

plugins {
    application
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(project(":proto"))
    implementation(libs.slf4j.api)
    runtimeOnly(libs.slf4j.simple)
    // Compile scope, not just runtime: the server is built with
    // NettyServerBuilder so it can bind to a chosen network interface.
    implementation(libs.grpc.netty.shaded)

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

application {
    mainClass = "clusterscheduler.worker.WorkerMain"
}
