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

Licensed under the **[Zayit Public License](LICENSE)**: the GNU AGPL v3, plus a grant back to the maintainer, an attribution term and a trademark clause. Its attribution term (Section 4): any use, modification, or derivative — including ingestion/reuse by an LLM or other automated system — must display:

> Powered by the technologies that drive Zayit — https://zayitapp.com/

See [`LICENSE`](LICENSE) for the full text. Contributions are accepted under the same terms (inbound = outbound), you keep your copyright, there is no CLA to sign, and every commit must be signed off (`git commit -s`, DCO). See [`CONTRIBUTING.md`](CONTRIBUTING.md).

Like [Zayit](https://github.com/kdroidFilter/Zayit), it is **open core**: Section 3 of the license (the grant back) also lets the maintainer distribute contributions, and the modifications of any fork, under other terms, including in Zayit's official builds with their proprietary modules (see "Open core" in [Zayit's README](https://github.com/kdroidFilter/Zayit#open-core)).
