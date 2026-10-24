package pgsearchsync.e2e

import com.trendyol.stove.database.migrations.DatabaseMigration
import com.trendyol.stove.elasticsearch.ElasticContainerOptions
import com.trendyol.stove.elasticsearch.ElasticsearchSystemOptions
import com.trendyol.stove.elasticsearch.elasticsearch
import com.trendyol.stove.extensions.kotest.StoveKotestExtension
import com.trendyol.stove.http.HttpClientSystemOptions
import com.trendyol.stove.http.httpClient
import com.trendyol.stove.kafka.KafkaContainerOptions
import com.trendyol.stove.kafka.KafkaMigrationContext
import com.trendyol.stove.kafka.KafkaSystemOptions
import com.trendyol.stove.kafka.kafka
import com.trendyol.stove.postgres.PostgresSqlMigrationContext
import com.trendyol.stove.postgres.PostgresqlContainerOptions
import com.trendyol.stove.postgres.PostgresqlOptions
import com.trendyol.stove.postgres.postgresql
import com.trendyol.stove.process.ProcessTarget
import com.trendyol.stove.system.ReadinessStrategy
import com.trendyol.stove.process.envMapper
import com.trendyol.stove.process.goApp
import com.trendyol.stove.system.Stove
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import org.apache.kafka.clients.admin.NewTopic
import java.io.File

const val APP_PORT = 18080
const val INDEX = "products"

class StoveConfig : AbstractProjectConfig() {
  override val extensions: List<Extension> = listOf(StoveKotestExtension())

  override suspend fun beforeProject() {
    Stove()
      .with {
        httpClient { HttpClientSystemOptions(baseUrl = "http://localhost:$APP_PORT") }

        postgresql {
          PostgresqlOptions(
            databaseName = "catalog",
            username = "app",
            password = "app",
            container = PostgresqlContainerOptions(tag = "17") {
              withCommand("postgres", "-c", "wal_level=logical", "-c", "fsync=off")
            },
            configureExposedConfiguration = { cfg ->
              listOf("pg.host=${cfg.host}", "pg.port=${cfg.port}")
            }
          ).migrations { register<CatalogSchema>() }
        }

        kafka {
          KafkaSystemOptions(
            containerOptions = KafkaContainerOptions(tag = "8.1.0"),
            configureExposedConfiguration = { cfg -> listOf("kafka.brokers=${cfg.bootstrapServers}") }
          ).migrations { register<Topics>() }
        }

        elasticsearch {
          ElasticsearchSystemOptions(
            container = ElasticContainerOptions(tag = "8.19.4") {
              withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            },
            configureExposedConfiguration = { cfg -> listOf("es.url=http://${cfg.host}:${cfg.port}") }
          )
        }

        goApp(
          target = ProcessTarget.Server(
            port = APP_PORT,
            portEnvVar = "PORT",
            readiness = ReadinessStrategy.HttpGet(url = "http://localhost:$APP_PORT/readyz")
          ),
          envProvider = envMapper {
            "pg.host" to "PG_HOST"
            "pg.port" to "PG_PORT"
            "kafka.brokers" to "KAFKA_BROKERS"
            "es.url" to "ES_URL"
            env("METRIC_PORT", "18112")
            env("PRODUCER_BATCH_TICK", "20ms")
            env("BULK_RETRY_FOR", "60s")
          }
        )
      }.run()
  }

  override suspend fun afterProject() {
    Stove.stop()
  }
}

/** The same schema the compose stack loads. */
class CatalogSchema : DatabaseMigration<PostgresSqlMigrationContext> {
  override val order: Int = 1

  override suspend fun execute(connection: PostgresSqlMigrationContext) {
    connection.operations.execute(File(System.getProperty("init.sql")).readText())
  }
}

class Topics : DatabaseMigration<KafkaMigrationContext> {
  override val order: Int = 1

  override suspend fun execute(connection: KafkaMigrationContext) {
    connection.admin
      .createTopics(listOf("products.cdc", "products.retry", "products.dlq").map { NewTopic(it, 3, 1.toShort()) })
      .all()
      .get()
  }
}
