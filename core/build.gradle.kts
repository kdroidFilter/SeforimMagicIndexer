plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.sqlDelight)
}

kotlin {
    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.serialization.json)
        }
    }
}

sqldelight {
    databases {
        create("Database") {
            // Database configuration here.
            // https://cashapp.github.io/sqldelight
            packageName.set("io.github.kdroidfilter.seforim.magicindexer.db")
        }
    }
}