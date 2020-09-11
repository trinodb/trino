import groovy.lang.Closure
import org.gradle.api.JavaVersion.VERSION_11

plugins {
    id("org.jetbrains.intellij") version "0.4.22"
    id("com.palantir.git-version") version "0.12.3"
}

val gitVersion: Closure<*> by ext
version = gitVersion()

repositories {
    jcenter()
}

java {
    sourceCompatibility = VERSION_11
    targetCompatibility = VERSION_11
}

intellij {
    version = "2020.2"
    setPlugins("TestNG-J", "java")
    updateSinceUntilBuild = false
}
tasks.buildSearchableOptions.get().enabled = false  // slow and not used

// RemoteTestNGStarter needs patching to work
val generatedDir = File("$buildDir/generated/starter")
val createDockerRemoteTestNGStarter by tasks.creating(Task::class) {
    val dockerRemoteTestNGStarterFile = File("$generatedDir/com/intellij/rt/testng/DockerRemoteTestNGStarter.java")
    outputs.file(dockerRemoteTestNGStarterFile)
    doLast {
        dockerRemoteTestNGStarterFile.parentFile.mkdirs()
        val srcUri = uri("https://raw.githubusercontent.com/JetBrains/intellij-community/d2ef69f336b62015bbefbb2c0a9900563c94062c/plugins/testng_rt/src/com/intellij/rt/testng/RemoteTestNGStarter.java")
        dockerRemoteTestNGStarterFile.writeText(srcUri.toURL().readText()
                .replace("RemoteTestNGStarter", "DockerRemoteTestNGStarter")
                .replace("127.0.0.1", "host.docker.internal"))
    }
}
sourceSets.main.get().java.srcDir(generatedDir)
tasks.compileJava.get().dependsOn(createDockerRemoteTestNGStarter)

val installToIdea by tasks.creating(Copy::class) {
    group = "intellij"
    dependsOn(tasks.buildPlugin)
    from(zipTree(tasks.buildPlugin.get().outputs.files.singleFile))
    val ideaUserHomePath = project.properties["ideaUserHome"] as String? ?: System.getenv("__INTELLIJ_COMMAND_HISTFILE__")?.substringBefore("/terminal/history/history-")
    if (ideaUserHomePath != null) {
        val pluginsDir = File(ideaUserHomePath, "plugins")
        into(pluginsDir)
        doFirst {
            logger.warn("Installing the plugin into $pluginsDir...")
            File(pluginsDir, project.name).deleteRecursively()
        }
    } else {
        into("unused")
        doFirst {
            throw IllegalStateException("'./gradlew $name' has to be run from IntelliJ terminal (View > Tool windows > Terminal), or with './gradlew $name -PideaUserHome=...'")
        }
    }
}
