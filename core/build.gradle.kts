plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.sqlDelight)
}

kotlin {
    jvm()

    sourceSets {
        commonMain.dependencies {
            // No additional dependencies needed for the schema
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