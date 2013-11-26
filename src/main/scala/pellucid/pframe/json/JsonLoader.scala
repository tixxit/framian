package pellucid.pframe
package json

import java.io.{ File, FileInputStream, InputStreamReader, BufferedReader, IOException }
import java.nio.charset.Charset
import scala.annotation.tailrec

import spire.std.int._

trait JsonLoader extends JsonModule {
  import JsonColumn._

  private final case class Schema[+A](value: Option[A], obj: Map[String, Schema[A]], arr: List[Schema[A]]) {
    def fold[B](f: A => B, g: (Option[B], Map[String, B], List[B]) => B): B =
      g(value map f, obj mapValues { _ fold(f, g) }, arr map { _ fold(f, g) })
  }

  private object Schema {
    def empty[A] = Schema[A](None, Map.empty, Nil)
  }

  private final class RowPopulator(schema: Schema[JsonColumn], row: Int) extends JsonVisitor[Schema[JsonColumn]] {
    def jsonObject(values: Iterable[(String, JsonValue)]): Schema[JsonColumn] =
      schema.copy(obj = values.foldLeft(schema.obj) { case (fields, (name, elem)) =>
        fields + (name -> visitJson(new RowPopulator(fields.getOrElse(name, Schema.empty), row))(elem))
      })

    def jsonArray(values: Seq[JsonValue]): Schema[JsonColumn] = {
      @tailrec
      def loop(i: Int, schemas: List[Schema[JsonColumn]], stack: List[Schema[JsonColumn]]): List[Schema[JsonColumn]] = {
        if (i < values.size) {
          schemas match {
            case Nil =>
              loop(i + 1, Nil, visitJson(new RowPopulator(Schema.empty, row))(values(i)) :: stack)
            case elemSchema :: tail =>
              loop(i + 1, tail, visitJson(new RowPopulator(elemSchema, row))(values(i)) :: stack)
          }
        } else {
          stack reverse_::: schemas
        }
      }

      schema.copy(arr = loop(0, schema.arr, Nil))
    }

    def jsonString(value: String): Schema[JsonColumn] =
      schema.copy(value = Some(Text(row, value, schema.value.getOrElse(Start))))
    def jsonNumber(value: BigDecimal): Schema[JsonColumn] =
      schema.copy(value = Some(Number(row, value, schema.value.getOrElse(Start))))
    def jsonBoolean(value: Boolean): Schema[JsonColumn] =
      schema.copy(value = Some(Bool(row, value, schema.value.getOrElse(Start))))
    def jsonNull(): Schema[JsonColumn] = schema
  }

  private def populate(schema: Schema[JsonColumn], row: Int, json: JsonValue): Schema[JsonColumn] =
    visitJson(new RowPopulator(schema, row))(json)

  def load(file: File): Either[JsonError, Frame[Int, JsonPath]] = try {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")))
    val jsonStr = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
    reader.close()
    parseToFrame(jsonStr)
  } catch { case (e: IOException) =>
    Left(JsonError.ioError(e))
  }

  def parseToFrame(jsonStr: String): Either[JsonError, Frame[Int, JsonPath]] =
    parseSeq(jsonStr).right map jsonToFrame

  def jsonToFrame(objs: Seq[JsonValue]): Frame[Int, JsonPath] = {
    type FlatJson = Iterable[(JsonPath, UntypedColumn)]

    val schema = objs.zipWithIndex.foldLeft(Schema.empty[JsonColumn]) { case (schema0, (json, row)) =>
      populate(schema0, row, json)
    }
    val columns = schema.fold[FlatJson](
      { col =>
        (JsonPath.root, col.toColumn) :: Nil
      },
      { (value, obj, arr) =>
        val fields: FlatJson = for {
            (name, selectors) <- obj
            (selector, column) <- selectors
          } yield ((name :: selector) -> column)
        val indices: FlatJson = for {
            (selectors, index) <- arr.zipWithIndex
            (selector, column) <- selectors
          } yield ((index :: selector) -> column)

        value.getOrElse(Iterable.empty) ++ fields ++ indices
      })
    Frame(Index(Array.range(0, objs.size)), columns.toList: _*)
  }
}
