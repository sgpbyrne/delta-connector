plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.ktlint) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.dokka)
}

repositories {
    mavenCentral()
}

dependencies {
    dokkaHtmlMultiModulePlugin("org.jetbrains.dokka:all-modules-page-plugin:${libs.versions.dokka.get()}")
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "org.jetbrains.dokka")

    group = "com.deltaconnect"
    version = rootProject.findProperty("version") as String? ?: "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        }
    }

    tasks.withType<JavaCompile> {
        sourceCompatibility = "17"
        targetCompatibility = "17"
    }

    dependencies {
        "implementation"(rootProject.libs.slf4j.api)

        "testImplementation"(rootProject.libs.bundles.testing)
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    val main = the<SourceSetContainer>()["main"]
    val test = the<SourceSetContainer>()["test"]

    val integrationTest = the<SourceSetContainer>().create("integrationTest") {
        compileClasspath += main.output + test.output
        runtimeClasspath += main.output + test.output
    }

    configurations[integrationTest.implementationConfigurationName]
        .extendsFrom(configurations[test.implementationConfigurationName])
    configurations[integrationTest.runtimeOnlyConfigurationName]
        .extendsFrom(configurations[test.runtimeOnlyConfigurationName])

    tasks.matching { it.name == "compileIntegrationTestKotlin" }.configureEach {
        (this as org.jetbrains.kotlin.gradle.tasks.KotlinCompile).compilerOptions {
            freeCompilerArgs.add("-Xfriend-paths=${main.output.classesDirs.asPath}")
        }
    }

    tasks.register<Test>("integrationTest") {
        description = "Runs integration tests with Testcontainers."
        group = "verification"
        testClassesDirs = integrationTest.output.classesDirs
        classpath = integrationTest.runtimeClasspath
        useJUnitPlatform()
        shouldRunAfter(tasks.named("test"))
    }

    apply(plugin = "jacoco")

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showCauses = true
            showStackTraces = true
        }
        afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ desc, result ->
            if (desc.parent == null) {
                println(
                    "\nTests: ${result.testCount} total, " +
                        "${result.successfulTestCount} passed, " +
                        "${result.failedTestCount} failed, " +
                        "${result.skippedTestCount} skipped " +
                        "(${result.endTime - result.startTime}ms)"
                )
            }
        }))
        finalizedBy(tasks.withType<JacocoReport>())
    }

    tasks.withType<JacocoReport> {
        dependsOn(tasks.named("test"))
        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
    }

    configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
        version.set("1.5.0")
    }

    configure<io.gitlab.arturbosch.detekt.extensions.DetektExtension> {
        config.setFrom(rootProject.files("detekt.yml"))
        buildUponDefaultConfig = true
        parallel = true
    }
}
