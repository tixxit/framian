package framian

import org.specs2.mutable._

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuilder

import spire.algebra._
import spire.std.string._
import spire.std.int._
import spire.syntax.monoid._

class IndexSpec extends Specification {
  "Index construction" should {
    def checkIndex(idx: Index[String])(pairs: (String, Int)*) = {
      idx.toList must_== pairs.toList
      (pairs map (_._1) map (idx search _)) must_== (pairs map (_._2))
    }

    "empty" in {
      checkIndex(Index.empty[String])()
    }

    "from keys only" in {
      checkIndex(Index.fromKeys("a", "b", "c"))("a" -> 0, "b" -> 1, "c" -> 2)
    }

    "from pairs" in {
      checkIndex(Index("a" -> 5, "c" -> 0, "b" -> 2))("a" -> 5, "c" -> 0, "b" -> 2)
    }

    "key/index array pair" in {
      checkIndex(Index(Array("a", "c", "b"), Array(5, 0, 2)))("a" -> 5, "c" -> 0, "b" -> 2)
    }

    "ordered with keys only" in {
      checkIndex(Index.ordered(Array("a", "b", "c")))("a" -> 0, "b" -> 1, "c" -> 2)
    }

    "ordered with indices" in {
      checkIndex(Index.ordered(Array("a", "b", "c"), Array(5, 0, 2)))("a" -> 5, "b" -> 0, "c" -> 2)
    }
  }

  "Index" should {
    "traverse elements in original order" in {
      val idx = Index.fromKeys("c", "a", "b")
      idx.toList map (_._1) must_== List("c", "a", "b")
    }

    "return the index of an element in the original order" in {
      val idx = Index.fromKeys("c", "a", "b")
      idx get "a" must_== Some(1)
      idx get "b" must_== Some(2)
      idx get "c" must_== Some(0)
    }

    "allow access by iteration order" in {
      val idx = Index.fromKeys("c", "a", "b")
      idx(0) must_== ("c" -> 0)
      idx(1) must_== ("a" -> 1)
      idx(2) must_== ("b" -> 2)
    }

    "foreach iterates in order" in {
      val idx = Index("c" -> 1, "a" -> 3, "b" -> 2)
      val bldr = ArrayBuilder.make[(String, Int)]
      idx foreach { (k, i) =>
        bldr += (k -> i)
      }
      bldr.result() must_== Array("c" -> 1, "a" -> 3, "b" -> 2)
    }

    "sorted puts things in order" in {
      val idx = Index.fromKeys("c", "a", "b").sorted
      idx.toList must_== List("a" -> 1, "b" -> 2, "c" -> 0)
    }
  }

  "Index.Grouper" should {
    class TestGrouper[K] extends Index.Grouper[K] {
      case class State(groups: Vector[List[(K, Int)]])

      def init = State(Vector.empty)

      def group(state: State)(keys: Array[K], indices: Array[Int], start: Int, end: Int): State = {
        State(state.groups :+ (keys zip indices).drop(start).take(end - start).toList)
      }
    }

    val grouper = new TestGrouper[String]

    "group empty index" in {
      val idx = Index[String]()
      Index.group(idx)(grouper).groups must_== Vector.empty[(String, Int)]
    }

    "trivial grouping" in {
      val idx = Index.fromKeys("a", "a", "a")
      val expected = Vector(List("a" -> 0, "a" -> 1, "a" -> 2))
      Index.group(idx)(grouper).groups must_== expected
    }

    "group in-order elements" in {
      val idx = Index.fromKeys("a", "b", "c")
      val expected = Vector(List("a" -> 0), List("b" -> 1), List("c" -> 2))
      Index.group(idx)(grouper).groups must_== expected
    }

    "group out-of-order elements" in {
      val idx = Index.fromKeys("a", "b", "a", "b")
      val expected = Vector(List("a" -> 0, "a" -> 2), List("b" -> 1, "b" -> 3))
      Index.group(idx)(grouper).groups must_== expected
    }
  }

  "Index.Cogrouper" should {
    type Cogroup[K] = (List[(K, Int)], List[(K, Int)])

    class TestCogrouper[K: ClassTag] extends Index.Cogrouper[K] {
      case class State(groups: Vector[Cogroup[K]])
      def init = State(Vector.empty)

      def cogroup(state: State)(
          lKeys: Array[K], lIdx: Array[Int], lStart: Int, lEnd: Int,
          rKeys: Array[K], rIdx: Array[Int], rStart: Int, rEnd: Int): State = {
        val lGroup = (lKeys zip lIdx).drop(lStart).take(lEnd - lStart).toList
        val rGroup = (rKeys zip rIdx).drop(rStart).take(rEnd - rStart).toList
        State(state.groups :+ (lGroup, rGroup))
      }
    }

    val cogrouper = new TestCogrouper[String]

    "cogroup empty indices" in {
      val idx = Index[String]()
      val expected = Vector.empty[Cogroup[String]]
      Index.cogroup(idx, idx)(cogrouper).groups must_== expected
    }

    "cogroup with 1 group on left" in {
      val lhs = Index.fromKeys("a", "a")
      val rhs = Index[String]()
      val expected = Vector[Cogroup[String]]((List("a" -> 0, "a" -> 1), List()))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }

    "cogroup with 1 group on right" in {
      val lhs = Index[String]()
      val rhs = Index.fromKeys("a", "a")
      val expected = Vector[Cogroup[String]]((List(), List("a" -> 0, "a" -> 1)))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }

    "cogroup with 1 shared group" in {
      val lhs = Index.fromKeys("a", "a")
      val rhs = Index("a" -> 2)
      val expected = Vector[Cogroup[String]]((List("a" -> 0, "a" -> 1), List("a" -> 2)))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }

    "cogroup missing group on left" in {
      val lhs = Index.fromKeys("a", "a")
      val rhs = Index.fromKeys("a", "b")
      val expected = Vector[Cogroup[String]]((List("a" -> 0, "a" -> 1), List("a" -> 0)),
        (List(), List("b" -> 1)))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }

    "cogroup in-order indices" in {
      val lhs = Index.fromKeys("a", "a", "b")
      val rhs = Index.fromKeys("a", "b", "c")
      val expected = Vector[Cogroup[String]](
        (List("a" -> 0, "a" -> 1), List("a" -> 0)),
        (List("b" -> 2), List("b" -> 1)),
        (List(), List("c" -> 2)))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }

    "cogroup out-of-order indices" in {
      val lhs = Index.fromKeys("a", "b", "a", "b")
      val rhs = Index.fromKeys("c", "a", "b", "c")
      val expected = Vector[Cogroup[String]](
        (List("a" -> 0, "a" -> 2), List("a" -> 1)),
        (List("b" -> 1, "b" -> 3), List("b" -> 2)),
        (List(), List("c" -> 0, "c" -> 3)))
      Index.cogroup(lhs, rhs)(cogrouper).groups must_== expected
    }
  }
}
