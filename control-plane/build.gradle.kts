// The control plane: accepts jobs, tracks nodes, and (on the leader) runs the
// scheduling and reconcile loops. All of its state lives in PostgreSQL.

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
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.slf4j.simple)
    // Compile scope, not just runtime: the server is built with
    // NettyServerBuilder so it can bind to a chosen network interface.
    implementation(libs.grpc.netty.shaded)

    // Integration tests start real workers against a real PostgreSQL.
    testImplementation(project(":worker"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
    }
}

application {
    mainClass = "clusterscheduler.controlplane.ControlPlaneMain"
}
