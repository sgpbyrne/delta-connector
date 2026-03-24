dependencies {
    implementation(project(":delta-protocol"))
    implementation(libs.azure.storage.datalake)
    implementation(libs.azure.identity)
    implementation(libs.bundles.jackson)
    implementation(libs.bundles.parquet)

    // StorageProvider interface (provided by delta-connect at runtime)
    compileOnly(project(":delta-connect"))
    compileOnly(libs.kafka.connect.api)

    testImplementation(project(":delta-connect"))
    testImplementation(libs.kafka.connect.api)
    testImplementation(libs.slf4j.simple)
}
