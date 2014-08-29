import sbt._

object Boilerplate {
  def gen(dir: File): Seq[File] = {
    val denseColumnFunctions = dir / "framian" / "column" / "DenseColumnFunctions.scala"
    IO.write(denseColumnFunctions, GenDenseColumnFunctions.body)

    Seq(denseColumnFunctions)
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

    def configs =
      Config("Generic", "A", "A", false) ::
      Config("Any", "Any", "A", false, ".asInstanceOf[A]") ::
      specs.map(s => Config(s, s, s, true))

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
      |package framian.column
      |
      |import scala.collection.immutable.BitSet
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
      |  private def copyArray[A](xs: Array[A], len: Int): Array[A] = xs match {
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
      |}
    """.stripMargin
  }

  object GenReindex extends GenSpecFunction(false) {
    def gen(config: Config): String = {
      import config._

      def typeParams: String = if (isSpec) s"" else s"[$inputType]"

      block"""
        |  def reindex$name$typeParams(index: Array[Int], values: Array[$inputArrayType], naValues: BitSet, nmValues: BitSet): Column[$inputType] = {
        |    val xs = copyArray(values, index.length)
        |    val na = BitSet.newBuilder
        |    val nm = BitSet.newBuilder
        |    var i = 0
        |    while (i < index.length) {
        |      val row = index(i)
        |      if (naValues(row)) na += i
        |      else if (nmValues(row)) nm += i
        |      else if (row >= 0 && row < values.length) xs(i) = values(row)$cast
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
        |  def map$name[$typeParams](values: Array[$inputArrayType], naValues: BitSet, nmValues: BitSet, f: $inputType => B): Column[B] = {
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
        |        NAColumn[B](nmValues)
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
        |    val na = BitSet.newBuilder
        |    val nm = BitSet.newBuilder
        |
        |    def loop(i: Int): Column[A] =
        |      if (i < len) {
        |        col(i) match {
        |          case NA => na += i; loop(i + 1)
        |          case NM => na += i; loop(i + 1)
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
        |        NAColumn[A](nm.result())
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
}
