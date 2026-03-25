plugins {
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":delta-protocol"))
    implementation(project(":delta-azure"))
    implementation(project(":delta-catalog"))
    implementation(project(":delta-connect"))
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("delta-sink-connector")
    archiveClassifier.set("all")

    // Kafka Connect runtime provides these - exclude to avoid classpath conflicts
    dependencies {
        exclude(dependency("org.apache.kafka:connect-api"))
        exclude(dependency("org.apache.kafka:connect-json"))
        exclude(dependency("org.apache.kafka:kafka-clients"))
        exclude(dependency("org.slf4j:slf4j-api"))
    }

    // Relocate dependencies that commonly conflict with the Connect runtime
    relocate("com.fasterxml.jackson", "com.deltaconnect.shaded.jackson")
    relocate("com.google.protobuf", "com.deltaconnect.shaded.protobuf")
    relocate("io.micrometer", "com.deltaconnect.shaded.micrometer")

    // Merge service loader descriptors
    mergeServiceFiles()

    manifest {
        attributes(
            "Implementation-Title" to "Delta Sink Connector",
            "Implementation-Version" to project.version
        )
    }
}

// Make shadow jar the default artifact
tasks.named("build") {
    dependsOn(tasks.named("shadowJar"))
}
