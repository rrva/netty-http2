plugins {
    id("org.jetbrains.kotlin.jvm") version "1.8.20"

    application
}

repositories {
    mavenCentral()
}


kotlin {
    jvmToolchain(17)
}

dependencies {

    implementation("ch.qos.logback:logback-classic:1.4.6")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.14.2")

    implementation("io.netty:netty-codec-http2:4.1.91.Final")
    implementation("io.netty:netty-handler:4.1.91.Final")

}