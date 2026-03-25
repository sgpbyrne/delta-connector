plugins {
    alias(libs.plugins.shadow)
    `maven-publish`
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
            "Implementation-Version" to project.version,
        )
    }
}

// Make shadow jar the default artifact
tasks.named("build") {
    dependsOn(tasks.named("shadowJar"))
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            val repo = System.getenv("GITHUB_REPOSITORY") ?: "deltaconnect/delta-sink-connector"
            url = uri("https://maven.pkg.github.com/$repo")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
    publications {
        create<MavenPublication>("shadow") {
            artifact(tasks.named("shadowJar"))
            groupId = project.group.toString()
            artifactId = "delta-sink-connector"
            version = project.version.toString()
        }
    }
}
