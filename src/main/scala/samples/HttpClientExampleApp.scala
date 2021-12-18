package samples

import com.sksamuel.elastic4s.{
  ElasticClient,
  ElasticProperties,
  RequestFailure,
  RequestSuccess
}
import com.sksamuel.elastic4s.fields.{KeywordField, NestedField, TextField}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.searches.SearchResponse

object HttpClientExampleApp extends App {

  // you must import the DSL to use the syntax helpers
  import com.sksamuel.elastic4s.ElasticDsl._

  val client = ElasticClient(
    JavaClient(
      ElasticProperties(
        s"http://${sys.env.getOrElse("ES_HOST", "127.0.0.1")}:${sys.env
          .getOrElse("ES_PORT", "9200")}"
      )
    )
  )

//  client.execute {
//    createIndex("artists").mapping(
//      properties(TextField("name"))
//    )
//  }.await
//
//  client.execute {
//    indexInto("artists")
//      .fields("name" -> "L.S. Lowry")
//      .refresh(RefreshPolicy.Immediate)
//  }.await
//
//  val resp = client.execute {
//    search("artists").query("lowry")
//  }.await
//
//  println("---- Search Results ----")
//  resp match {
//    case failure: RequestFailure => println("We failed " + failure.error)
//    case results: RequestSuccess[SearchResponse] =>
//      println(results.result.hits.hits.toList)
//    case results: RequestSuccess[_] => println(results.result)
//  }
//
//  resp foreach (search => println(s"There were ${search.totalHits} total hits"))

  client.execute {
    createIndex("samples").mapping(
      properties(
        KeywordField("foo"),
        NestedField(
          "parts",
          properties = Seq(
            KeywordField("status")
          )
        )
      )
    )
  }.await

  client.execute {
    bulk(
      indexInto("samples")
        .fields(
          "foo" -> "xxxx",
          "parts" -> Seq(
            Map("status" -> "Test"),
            Map("status" -> "Test"),
            Map("status" -> "Hoge")
          )
        ),
      indexInto("samples")
        .fields(
          "foo" -> "xxxx",
          "parts" -> Seq(
            Map("status" -> "Foo"),
            Map("status" -> "Bar"),
            Map("status" -> "Hoge")
          )
        ),
      indexInto("samples")
        .fields(
          "foo" -> "xxxx",
          "parts" -> Seq(
            Map("status" -> "Bar"),
            Map("status" -> "Bar"),
            Map("status" -> "Bar")
          )
        )
    ).refresh(RefreshPolicy.Immediate)
  }.await

  val x = client.execute {
    search("samples")
  }.await

  println("---- Search Results ----")
  x match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] =>
      println(results.result.hits.hits.toList)
    case results: RequestSuccess[_] => println(results.result)
  }

  val resp = client.execute {
    search("samples").size(0).aggs {
      nestedAggregation("parts", "parts").subaggs {
        termsAgg("status", "parts.status").subaggs {
          valueCountAgg("total", "parts.status")
        }
      }
    }
  }.await

  resp foreach (search =>
    println(s"There were ${search.aggregationsAsString} total hits")
  )

  client.execute {
    deleteIndex("samples")
  }.await

  client.close()
}
