package pgsearchsync.e2e

import com.trendyol.stove.elasticsearch.elasticsearch
import com.trendyol.stove.http.http
import com.trendyol.stove.postgres.postgresql
import com.trendyol.stove.system.stove
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds

data class Product(
  val id: Long = 0,
  val sku: String = "",
  val title: String = "",
  val brand: String = "",
  val category: String = "",
  val price_cents: Long = 0,
  val stock: Int = 0,
  val version: Long = 0,
  val updated_at: String = "",
  val deleted: Boolean = false
)

data class SearchResponse(val total: Long = 0, val items: List<Product> = emptyList())

class SearchSyncTest :
  FunSpec({
    test("a product written to PostgreSQL becomes searchable, updates and deletes follow") {
      stove {
        postgresql {
          shouldExecute(
            """INSERT INTO products (id, sku, title, brand, category, price_cents, stock)
               VALUES (101, 'E2E-101', 'Koton oversize hoodie', 'Koton', 'hoodie', 49900, 4)"""
          )
        }
        eventually(30.seconds) {
          http {
            get<SearchResponse>("/search", queryParams = mapOf("q" to "oversize hoodie")) { r ->
              r.total shouldBe 1
              r.items.single().sku shouldBe "E2E-101"
            }
          }
        }

        postgresql { shouldExecute("UPDATE products SET price_cents = 39900 WHERE id = 101") }
        eventually(30.seconds) {
          elasticsearch {
            shouldGet<Product>(index = INDEX, key = "101") { it.price_cents shouldBe 39900 }
          }
        }

        postgresql { shouldExecute("DELETE FROM products WHERE id = 101") }
        eventually(30.seconds) {
          elasticsearch {
            // Deletes are tombstones, so a late retry of an older version cannot resurrect the row.
            shouldGet<Product>(index = INDEX, key = "101") { it.deleted shouldBe true }
          }
          http {
            get<SearchResponse>("/search", queryParams = mapOf("q" to "hoodie")) { r -> r.total shouldBe 0 }
          }
        }
      }
    }

    test("writes made while Elasticsearch is paused arrive after it comes back") {
      stove {
        elasticsearch { pause() }
        postgresql {
          for (id in 201..220) {
            shouldExecute(
              """INSERT INTO products (id, sku, title, brand, category, price_cents, stock)
                 VALUES ($id, 'PAUSE-$id', 'Mavi paused jeans $id', 'Mavi', 'jeans', 59900, 1)"""
            )
          }
        }
        Thread.sleep(10_000) // the indexer is now retrying against a frozen node
        elasticsearch { unpause() }
        eventually(90.seconds) {
          http {
            get<SearchResponse>("/search", queryParams = mapOf("q" to "paused", "limit" to "100")) { r ->
              r.total shouldBe 20
            }
          }
        }
      }
    }
  })
