// Black-box e2e tests with Trendyol Stove: Stove starts PostgreSQL, Kafka and
// Elasticsearch containers, runs the compiled cmd/all binary as the
// application under test and asserts on its HTTP API and on Elasticsearch.
plugins {
  kotlin("jvm") version "2.4.21"
}

val stoveVersion = "0.27.5"
val goBinary = layout.buildDirectory.file("pg-search-sync-all").get().asFile

tasks.register<Exec>("buildGoApp") {
  workingDir = rootDir.parentFile
  commandLine("go", "build", "-race", "-o", goBinary.absolutePath, "./cmd/all")
  inputs.files(fileTree(rootDir.parentFile) { include("cmd/**/*.go", "internal/**/*.go", "go.mod", "go.sum") })
  outputs.file(goBinary)
}

dependencies {
  testImplementation(platform("com.trendyol:stove-bom:$stoveVersion"))
  testImplementation("com.trendyol:stove")
  testImplementation("com.trendyol:stove-process")
  testImplementation("com.trendyol:stove-postgres")
  testImplementation("com.trendyol:stove-kafka")
  testImplementation("com.trendyol:stove-elasticsearch")
  testImplementation("com.trendyol:stove-http")
  testImplementation("com.trendyol:stove-extensions-kotest")
  testImplementation("io.kotest:kotest-runner-junit6:6.2.6")
  testImplementation("io.kotest:kotest-assertions-core:6.2.6")
}

kotlin { jvmToolchain(21) }

tasks.test {
  dependsOn("buildGoApp")
  useJUnitPlatform()
  systemProperty("go.app.binary", goBinary.absolutePath)
  systemProperty("init.sql", rootDir.parentFile.resolve("deploy/postgres/init.sql").absolutePath)
  testLogging { events("passed", "failed", "skipped"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL; showStandardStreams = false }
}
