plugins {
    // Downloads JDK 21 automatically when it is not installed, so the build
    // does not depend on whichever Java version the machine happens to have.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "cluster-scheduler"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("proto")
include("control-plane")
include("worker")
