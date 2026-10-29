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
    runtimeOnly(libs.grpc.netty.shaded)
}

application {
    mainClass = "clusterscheduler.worker.WorkerMain"
}
