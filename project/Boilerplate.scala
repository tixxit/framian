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

  trait GenSpecFunction {
    case class Config(name: String, inputArrayType: String, inputType: String, isSpec: Boolean, cast: String = "") {
      def typeParams: String = if (isSpec) "B" else s"$inputType, B"
    }

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
      |${GenMap.body}
      |}
    """.stripMargin
  }

  object GenMap extends GenSpecFunction {
    def gen(config: Config): String = {
      import config._
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
}
