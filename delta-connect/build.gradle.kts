dependencies {
    implementation(project(":delta-protocol"))
    implementation(project(":delta-catalog"))

    compileOnly(libs.kafka.connect.api)

    implementation(libs.bundles.jackson)
    implementation(libs.bundles.parquet)
    implementation(libs.avro)
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.jmx)
    implementation(libs.micrometer.otlp)

    testImplementation(libs.kafka.connect.api)
    testImplementation(libs.hadoop.mapreduce.client.core) {
        exclude(group = "org.apache.curator")
        exclude(group = "org.apache.zookeeper")
        exclude(group = "org.apache.kerby")
        exclude(group = "org.eclipse.jetty")
    }
    testImplementation(libs.slf4j.simple)

    "integrationTestImplementation"(project(":delta-azure"))
    "integrationTestImplementation"(libs.testcontainers.core)
    "integrationTestImplementation"(libs.testcontainers.junit)
    "integrationTestImplementation"(libs.testcontainers.kafka)
    "integrationTestImplementation"("org.apache.kafka:kafka-clients:${libs.versions.kafka.get()}")
    "integrationTestImplementation"(libs.slf4j.simple)
}
