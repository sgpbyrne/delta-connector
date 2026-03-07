dependencies {
    implementation(libs.bundles.jackson)
    implementation(libs.bundles.parquet)
    implementation(libs.hadoop.common) {
        // Exclude heavy transitive deps not needed for Delta protocol
        exclude(group = "org.apache.curator")
        exclude(group = "org.apache.zookeeper")
        exclude(group = "org.apache.kerby")
        exclude(group = "org.eclipse.jetty")
    }
    implementation(libs.avro)
}
