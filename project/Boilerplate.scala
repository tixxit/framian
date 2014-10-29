/*  _____                    _
 * |  ___| __ __ _ _ __ ___ (_) __ _ _ __
 * | |_ | '__/ _` | '_ ` _ \| |/ _` | '_ \
 * |  _|| | | (_| | | | | | | | (_| | | | |
 * |_|  |_|  \__,_|_| |_| |_|_|\__,_|_| |_|
 *
 * Copyright 2014 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Boilerplate {
  def gen(dir: File): Seq[File] = {
    val denseColumnFunctions = dir / "framian" / "column" / "DenseColumnFunctions.scala"
    IO.write(denseColumnFunctions, GenDenseColumnFunctions.body)

    val columnBuilders = dir / "framian" / "column" / "columnBuilders.scala"
    IO.write(columnBuilders, GenColumnBuilders.body)

    Seq(denseColumnFunctions, columnBuilders)
  }

  import scala.StringContext._

  // Courtesy of Shapeless.
  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated split '\n'
      val trimmedLines = rawLines map { _ dropWhile (_.isWhitespace) }
      trimmedLines mkString "\n"
    }
  }

  abstract class GenSpecFunction(specFunction: Boolean) {
    case class Config(name: String, inputArrayType: String, inputType: String, isSpec: Boolean, cast: String = "")

    def specs = List("Int", "Long", "Double")

    val GenericConfig = Config("Generic", "A", "A", false)
    val AnyConfig = Config("Any", "Any", "A", false, ".asInstanceOf[A]")
    val SpecConfigs = specs.map(s => Config(s, s, s, true))

    def configs = GenericConfig :: AnyConfig :: SpecConfigs

    def body: String = {
      def loop(lines: List[String], chunks: Vector[String]): String = lines match {
        case head :: rest =>
          val tpe = head.charAt(0)
          val chunk0 = lines.takeWhile(_.head == tpe).map(_.tail).mkString("\n")
          val chunk = tpe match {
            case '-' => specs.map(s => chunk0.replace("{specType}", s)).mkString("\n")
            case _ => chunk0
          }
          loop(rest.dropWhile(_.head == tpe), chunks :+ chunk)

        case Nil =>
          chunks.mkString("\n")
      }

      loop(configs.map(gen).mkString.split("\n").toList.filterNot(_.isEmpty), Vector.empty)
    }

    def gen(config: Config): String
  }

  object GenDenseColumnFunctions {
    val body = s"""
      |package framian
      |package column
      |
      |import framian.{Cell,NA,NM,Value}
      |
      |trait DenseColumnFunctions {
      |  private def copyToAnyArray[A](xs: Array[A], len: Int): Array[Any] = {
      |    val ys = new Array[Any](xs.size)
      |    var i = 0
      |    while (i < xs.length && i < len) {
      |      ys(i) = xs(i)
      |      i += 1
      |    }
      |    ys
      |  }
      |
      |  def copyArray[A](xs: Array[A], len: Int): Array[A] = xs match {
      |    case (xs: Array[Boolean]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Double]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Float]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Char]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Long]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Int]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Short]) => java.util.Arrays.copyOf(xs, len)
      |    case (xs: Array[Byte]) => java.util.Arrays.copyOf(xs, len)
      |    case _ => java.util.Arrays.copyOf(xs.asInstanceOf[Array[A with AnyRef]], len).asInstanceOf[Array[A]]
      |  }
      |
      |${GenMap.body}
      |
      |${GenReindex.body}
      |
      |${GenForce.body}
      |
      |${GenOrElse.body}
      |}
    """.stripMargin
  }

  object GenReindex extends GenSpecFunction(false) {
    def gen(config: Config): String = {
      import config._

      def typeParams: String = if (isSpec) s"" else s"[$inputType]"

      block"""
        |  def reindex$name$typeParams(index: Array[Int], values: Array[$inputArrayType], naValues: Mask, nmValues: Mask): Column[$inputType] = {
        |    val xs = copyArray(values, index.length)
        |    val na = Mask.newBuilder
        |    val nm = Mask.newBuilder
        |    var i = 0
        |    while (i < index.length) {
        |      val row = index(i)
        |      if (nmValues(row)) nm += i
        |      else if (row >= 0 && row < values.length && !naValues(row)) xs(i) = values(row)$cast
        |      else na += i
        |      i += 1
        |    }
        |    ${name}Column(xs, na.result(), nm.result())
        |  }
      """
    }
  }

  object GenMap extends GenSpecFunction(true) {
    def gen(config: Config): String = {
      import config._

      def typeParams: String = {
        val sp = "@specialized(Int,Long,Double)"
        if (isSpec) s"$sp B" else s"$inputType, $sp B"
      }

      block"""
        |  def map$name[$typeParams](values: Array[$inputArrayType], naValues: Mask, nmValues: Mask, f: $inputType => B): Column[B] = {
        |    def loop(i: Int): Column[B] =
        |      if (i < values.length) {
        |        if (naValues(i) || nmValues(i)) {
        |          loop(i + 1)
        |        } else {
        |          f(values(i)$cast) match {
        -            case (x: {specType}) =>
        -              val xs = new Array[{specType}](values.size)
        -              xs(i) = x
        -              loop{specType}(xs, i + 1)
        |            case x =>
        |              val xs = new Array[Any](values.size)
        |              xs(i) = x
        |              loopAny(xs, i + 1)
        |          }
        |        }
        |      } else {
        |        Column.empty[B](nmValues)
        |      }
        |  
        -    def loop{specType}(xs: Array[{specType}], i0: Int): Column[B] = {
        -      var i = i0
        -      while (i < xs.length) {
        -        if (!(naValues(i) || nmValues(i))) {
        -          try {
        -            xs(i) = f(values(i)$cast).asInstanceOf[{specType}]
        -          } catch { case (_: ClassCastException) =>
        -            return loopAny(copyToAnyArray(xs, i), i)
        -          }
        -        }
        -        i += 1
        -      }
        -      {specType}Column(xs, naValues, nmValues).asInstanceOf[Column[B]]
        -    }
        -  
        |    def loopAny(xs: Array[Any], i: Int): Column[B] =
        |      if (i < xs.length) {
        |        if (naValues(i) || nmValues(i)) {
        |          loopAny(xs, i + 1)
        |        } else {
        |          xs(i) = f(values(i)$cast)
        |          loopAny(xs, i + 1)
        |        }
        |      } else {
        |        AnyColumn(xs, naValues, nmValues)
        |      }
        |  
        |    loop(0)
        |  }
      """
    }
  }

  object GenForce extends GenSpecFunction(true) {
    override def configs = Config("Any", "Any", "A", false) :: Nil

    def gen(config: Config): String = {
      import config._

      def typeParams: String = {
        val sp = "@specialized(Int,Long,Double)"
        if (isSpec) s"$sp B" else s"$inputType, $sp B"
      }

      block"""
        |  def force[$inputType](col: Int => Cell[$inputType], len: Int): Column[A] = {
        |    val na = Mask.newBuilder
        |    val nm = Mask.newBuilder
        |
        |    def loop(i: Int): Column[A] =
        |      if (i < len) {
        |        col(i) match {
        |          case NA => na += i; loop(i + 1)
        |          case NM => nm += i; loop(i + 1)
        |          case Value(value) =>
        |            value match {
        -              case (x: {specType}) =>
        -                val xs = new Array[{specType}](len)
        -                xs(i) = x
        -                loop{specType}(xs, i + 1)
        |              case x =>
        |                val xs = new Array[Any](len)
        |                xs(i) = x
        |                loopAny(xs, i + 1)
        |            }
        |        }
        |      } else {
        |        Column.empty(nm.result())
        |      }
        |  
        -    def loop{specType}(xs: Array[{specType}], i0: Int): Column[A] = {
        -      var i = i0
        -      while (i < xs.length) {
        -        col(i) match {
        -          case NA => na += i
        -          case NM => nm += i
        -          case Value(value) =>
        -            try {
        -              xs(i) = value.asInstanceOf[{specType}]
        -            } catch { case (_: ClassCastException) =>
        -              return loopAny(copyToAnyArray(xs, i), i)
        -            }
        -        }
        -        i += 1
        -      }
        -      {specType}Column(xs, na.result(), nm.result()).asInstanceOf[Column[A]]
        -    }
        -  
        |    def loopAny(xs: Array[Any], i0: Int): Column[A] = {
        |      var i = i0
        |      while (i < xs.length) {
        |        col(i) match {
        |          case NA => na += i
        |          case NM => nm += i
        |          case Value(value) => xs(i) = value
        |        }
        |        i += 1
        |      }
        |      AnyColumn(xs, na.result(), nm.result())
        |    }
        |  
        |    loop(0)
        |  }
      """
    }
  }

  object GenOrElse extends GenSpecFunction(false) {
    def gen(config: Config): String = {
      import config._

      def typeParams: String = {
        if (isSpec) s"A0 >: $inputType"
        else s"$inputType, A0 >: $inputType"
      }

      val classTagCase = if (config.name != "Generic") {
        block"""
        |      case ${name}Column(values0, naValues0, nmValues0) =>
        |        val len = spire.math.max(values.length, values0.length)
        |        val naL = naValues | Mask.range(values.length, len)
        |        val naR = naValues0 | Mask.range(values0.length, len)
        |        // We could save an op here, but we expect |NA| >> |NM.
        |        val nm = (nmValues & nmValues0) | (nmValues & naR) | (naL & nmValues0)
        |        val xs = copyArray(values, len)
        |        var i = 0
        |        while (i < values0.length) {
        |          if (i >= values.length || naValues(i) || nmValues(i))
        |            xs(i) = values0(i)
        |          i += 1
        |        }
        |        ${name}Column(xs, naL & naR, nm).asInstanceOf[Column[A0]]
        |
        """
      } else ""

      block"""
        |  def orElse$name[$typeParams](values: Array[$inputArrayType], naValues: Mask, nmValues: Mask, rhs: Column[A0]): Column[A0] = {
        |    rhs match {
        $classTagCase
        |      case (rhs: DenseColumn[_]) =>
        |        val bldr = Column.newBuilder[A0]()
        |        val len = spire.math.max(values.length, rhs.values.length)
        |        var i = 0
        |        while (i < len) {
        |          if (i < values.length && !naValues(i) && !nmValues(i)) {
        |            bldr.addValue(values(i)$cast)
        |          } else if (rhs.isValueAt(i)) {
        |            bldr.addValue(rhs.valueAt(i))
        |          } else if (nmValues(i)) {
        |            bldr.addNM()
        |          } else {
        |            bldr.add(rhs.nonValueAt(i))
        |          }
        |          i += 1
        |        }
        |        bldr.result()
        |
        |      case _ => // TODO: Add case for unboxed columns.
        |        Column.eval { row =>
        |          if (row >= 0 && row < values.length && !naValues(row) && !nmValues(row)) {
        |            Value(values(row)$cast)
        |          } else {
        |            rhs(row) match {
        |              case NA if nmValues(row) => NM
        |              case cell => cell
        |            }
        |          }
        |        }
        |    }
        |  }
      """
    }
  }

  object GenColumnBuilders {
    val body = s"""
      |package framian
      |package column
      |
      |import scala.collection.mutable.ArrayBuilder
      |import scala.reflect.ClassTag
      |
      |import framian.{Cell,NA,NM,Value}
      |
      |${GenSpecColumnBuilders.body}
      |${GenAnyColumnBuilder.body}
    """.stripMargin
  }

  object GenSpecColumnBuilders extends GenSpecFunction(true) {
    override def configs = GenericConfig :: SpecConfigs

    def gen(config: Config): String = {
      import config._

      def typeParams: String = if (isSpec) s"" else s"[$inputType: ClassTag]"

      block"""
        |class ${name}ColumnBuilder$typeParams extends ColumnBuilder[$inputType] {
        |  var i = 0
        |  var values: ArrayBuilder[$inputType] = ArrayBuilder.make()
        |  var na = Mask.newBuilder
        |  var nm = Mask.newBuilder
        |
        |  def addValue(a: $inputType): this.type = { values += a; i += 1; this }
        |  def addNA(): this.type = { na += i; values += null.asInstanceOf[$inputType]; i += 1; this }
        |  def addNM(): this.type = { nm += i; values += null.asInstanceOf[$inputType]; i += 1; this }
        |
        |  def add(cell: Cell[$inputType]): this.type = cell match {
        |    case Value(a) => addValue(a)
        |    case NA => addNA()
        |    case NM => addNM()
        |  }
        |
        |  def result() = ${name}Column(values.result(), na.result(), nm.result())
        |
        |  def clear(): Unit = { i = 0; values.clear(); na.clear(); nm.clear() }
        |
        |  override def sizeHint(size: Int): Unit = values.sizeHint(size)
        |}
        |
      """
    }
  }

  object GenAnyColumnBuilder extends GenSpecFunction(false) {
    override def configs = AnyConfig :: Nil

    def gen(config: Config): String = {
      import config._

      block"""
        |class AnyColumnBuilder[A] extends ColumnBuilder[A] {
        |  var i = 0
        |  var values: ArrayBuilder[Any] = ArrayBuilder.make()
        |  var na = Mask.newBuilder
        |  var nm = Mask.newBuilder
        |
        |  def addValue(a: A): this.type = {
        |    values += a
        |    i += 1
        |    this
        |  }
        |
        |  def addNA(): this.type = {
        |    na += i
        |    values += null
        |    i += 1
        |    this
        |  }
        |
        |  def addNM(): this.type = {
        |    nm += i
        |    values += null
        |    i += 1
        |    this
        |  }
        |
        |  def add(cell: Cell[A]): this.type = cell match {
        |    case Value(a) => addValue(a)
        |    case NA => addNA()
        |    case NM => addNM()
        |  }
        |
        |  def result(): Column[A] = {
        |    val data = values.result()
        |    val naValues = na.result()
        |    val nmValues = nm.result()
        |    val fallback = AnyColumn[A](data, naValues, nmValues)
        |
        |    def loop(i: Int): Column[A] =
        |      if (i < data.length) {
        |        if (naValues(i) || nmValues(i)) {
        |          loop(i + 1)
        |        } else {
        |          data(i) match {
        -            case (x: {specType}) =>
        -              val xs = new Array[{specType}](data.size)
        -              xs(i) = x
        -              loop{specType}(xs, i + 1)
        |            case x =>
        |              fallback
        |          }
        |        }
        |      } else {
        |        fallback
        |      }
        |
        -    def loop{specType}(xs: Array[{specType}], i: Int): Column[A] =
        -      if (i < data.length) {
        -        if (naValues(i) || nmValues(i)) {
        -          loop{specType}(xs, i + 1)
        -        } else data(i) match {
        -          case (x: {specType}) =>
        -            xs(i) = x
        -            loop{specType}(xs, i + 1)
        -          case _ =>
        -            fallback
        -        }
        -      } else {
        -        {specType}Column(xs, naValues, nmValues).asInstanceOf[Column[A]]
        -      }
        |
        |    loop(0)
        |  }
        |
        |  def clear(): Unit = { i = 0; values.clear(); na.clear(); nm.clear() }
        |
        |  override def sizeHint(size: Int): Unit = values.sizeHint(size)
        |}
      """
    }
  }
}
