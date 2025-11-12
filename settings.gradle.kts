rootProject.name = "SeforimMagicIndexer"

pluginManagement {
    repositories {
        google {
            content { 
              	includeGroupByRegex("com\\.android.*")
              	includeGroupByRegex("com\\.google.*")
              	includeGroupByRegex("androidx.*")
              	includeGroupByRegex("android.*")
            }
        }
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        google {
            content { 
              	includeGroupByRegex("com\\.android.*")
              	includeGroupByRegex("com\\.google.*")
              	includeGroupByRegex("androidx.*")
              	includeGroupByRegex("android.*")
            }
        }
        mavenCentral()
    }
}
include(":core")
include(":magicindexer")

// Include parent SeforimLibrary modules
includeBuild("..") {
    dependencySubstitution {
        substitute(module("io.github.kdroidfilter.seforimlibrary:core")).using(project(":core"))
        substitute(module("io.github.kdroidfilter.seforimlibrary:dao")).using(project(":dao"))
    }
}

