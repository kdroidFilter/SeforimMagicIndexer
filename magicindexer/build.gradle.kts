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
            implementation("com.google.genai:google-genai:1.26.0")
        }

        jvmTest.dependencies {
            implementation(kotlin("test"))

        }

    }

}
