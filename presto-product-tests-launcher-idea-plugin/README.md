# IntelliJ IDEA plugin for launching Presto product tests


## Installing the plugin

- In IntelliJ terminal (`View > Tool windows > Terminal`) run `./gradlew installToIdea`, or (in any terminal)
use `./gradlew installToIdea -DideaUserHome=<path-to-idea-user-directory>`

- Restart IntelliJ

- In IntelliJ, in `Edit configurations > Templates > TestNG > Environment variables` set (for example)
    `PT_ENVIRONMENT=singlenode;TESTCONTAINERS_REUSE_ENABLE=true`
    
- Mac only: add `/Applications/IntelliJ IDEA.app/Contents` to mountable directories in Docker settings

- Run tests normally (for example by clicking green arrow in the editor); you can also use IntelliJ debugger


## Building

`./gradlew assemble`


## Developing the plugin

- Install IntelliJ plugins: `Gradle`, `Kotlin`, `Plugin DevKit`
- Import this module to IntelliJ (eg with `File > New > Module from existing sources > Gradle`)


## Testing the plugin

`./gradlew runIde`

A second copy of IntelliJ opens. The logs are visible in the `Run` panel of the first IntelliJ. 
