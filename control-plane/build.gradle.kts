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
    runtimeOnly(libs.grpc.netty.shaded)
}

application {
    mainClass = "clusterscheduler.controlplane.ControlPlaneMain"
}
