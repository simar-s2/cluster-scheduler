// Holds the .proto files and the Java classes generated from them.
// The control plane and the worker both depend on this module, so they
// always agree on the wire format.

plugins {
    `java-library`
    alias(libs.plugins.protobuf)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // "api" rather than "implementation": the generated classes extend gRPC and
    // protobuf types, so any module that uses them needs these libraries too.
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().configureEach {
            plugins {
                create("grpc")
            }
        }
    }
}
