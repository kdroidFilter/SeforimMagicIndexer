@file:OptIn(ExperimentalKotlinGradlePluginApi::class)

import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvm {
        mainRun {
            mainClass = "io.github.kdroidfilter.seforim.magicindexer.MainKt"
        }
    }

    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
        }

        jvmMain.dependencies {
            // SeforimLibrary dependencies from parent build
            implementation("io.github.kdroidfilter.seforimlibrary:core")
            implementation("io.github.kdroidfilter.seforimlibrary:dao")

            // Other dependencies
            implementation(libs.kotlinx.coroutines.swing)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.kotlinx.serialization.json)
            implementation("com.google.genai:google-genai:1.34.0")
            implementation("org.jsoup:jsoup:1.21.2")
        }

        jvmTest.dependencies {
            implementation(kotlin("test"))

        }

    }

}

// Task to run the Hebrew diacritics post-processor
tasks.register<JavaExec>("runPostProcessor") {
    group = "application"
    description = "Run the Hebrew diacritics post-processor to clean nikud and taamim from the database"
    mainClass.set("io.github.kdroidfilter.seforim.magicindexer.HebrewDiacriticsPostProcessorKt")
    classpath = kotlin.jvm().compilations["main"].runtimeDependencyFiles
}
