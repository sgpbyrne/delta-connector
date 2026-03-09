dependencies {
    implementation(project(":delta-protocol"))
    implementation(libs.azure.storage.datalake)
    implementation(libs.azure.identity)
    implementation(libs.bundles.jackson)
    implementation(libs.bundles.parquet)

    testImplementation(libs.slf4j.simple)
}
