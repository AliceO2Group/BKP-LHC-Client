import java.nio.file.Paths
import kotlin.io.path.Path

plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(files("../libs/jdip-6.2.0.jar"))
    implementation(libs.protobuf.core)
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.jdk)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = "alice.dip.AliDip2BK"
}

tasks.named<JavaExec>("run") {
    doFirst {
        environment("LD_LIBRARY_PATH", rootDir.path + "/libs")
    }
}