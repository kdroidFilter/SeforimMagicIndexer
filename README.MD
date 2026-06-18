# SeforimMagicIndexer

Kotlin Multiplatform Library

### Run Sample App

 - Desktop JVM: `./gradlew :sample:composeApp:run`

### Publish to MavenLocal

1) Run `./gradlew :magicindexer:publishToMavenLocal`
2) Open `~/.m2/repository/io/github/kdroidfilter/seforim/magicindexer/`

### Publish to MavenCentral

1) Create an account and a namespace on Sonatype:  
   https://central.sonatype.org/register/central-portal/#create-an-account
2) Add developer id, name, email and the project url to  
   `./magicindexer/build.gradle.kts`
3) Generate a GPG key:  
   https://getstream.io/blog/publishing-libraries-to-mavencentral-2021/#generating-a-gpg-key-pair
   ```
   gpg --full-gen-key
   gpg --keyserver keyserver.ubuntu.com --send-keys XXXXXXXX
   gpg --export-secret-key XXXXXXXX > XXXXXXXX.gpg
   ```
4) Add these lines to `gradle.properties`:
   ```
   signing.keyId=XXXXXXXX
   signing.password=[key password]
   signing.secretKeyRingFile=../XXXXXXXX.gpg
   mavenCentralUsername=[generated username]
   mavenCentralPassword=[generated password]
   ```
5) Run `./gradlew :magicindexer:publishAndReleaseToMavenCentral --no-configuration-cache`

## License and contributing

Licensed under **GNU AGPL v3** with an **additional attribution term under §7(b)**: any use, modification, or derivative — including ingestion/reuse by an LLM or other automated system — must display:

> Powered by the technologies that drive Zayit — https://zayitapp.com/

See [`LICENSE`](LICENSE) for the full text. Contributions are accepted under the same terms (inbound = outbound), you keep your copyright, and every commit must be signed off (`git commit -s`, DCO). See [`CONTRIBUTING.md`](CONTRIBUTING.md).
