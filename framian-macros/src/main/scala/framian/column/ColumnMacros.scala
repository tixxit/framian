package framian
package column

import scala.language.experimental.macros

// Whitebox is required by Spire, but I don't think Spire needs it either.
import scala.reflect.macros.whitebox.Context

import spire.macros.{ SyntaxUtil, InlineUtil }

class ColumnMacros[C <: Context](val c: C) {
  import c.universe._

  val util = new SyntaxUtil[c.type](c)
  val inliner = new InlineUtil[c.type](c)

  def freshTermName(c: Context, prefix: String): c.universe.TermName =
    c.universe.TermName(c.freshName(prefix))

  private def sanitize[A](e: c.Expr[A]): (c.Tree, List[c.Tree]) =
    if (util.isClean(e)) {
      (e.tree, Nil)
    } else {
      val name = freshTermName(c, "norm$")
      (q"$name", List(q"val $name = $e"))
    }

  def foldRow[A, B](row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Expr[B] = {
    val col = freshTermName(c, "foldRow$col$")
    val value = freshTermName(c, "foldRow$value$")
    val r = freshTermName(c, "foldRow$row$")
    val (iter, prefix) = sanitize(f)

    val tree = q"""
    ${c.prefix} match {
      case ($col: _root_.framian.UnboxedColumn[_]) =>
        val $r = $row
        if ($col.isValueAt($r)) {
          $iter($col.valueAt($r))
        } else {
          $col.nonValueAt($r) match {
            case _root_.framian.NA => $na
            case _root_.framian.NM => $nm
          }
        }

      case $col =>
        $col($row) match {
          case _root_.framian.NA => $na
          case _root_.framian.NM => $nm
          case _root_.framian.Value($value) => $iter($value)
        }
    }
    """

    val block = Block(prefix, tree)
    inliner.inlineAndReset[B](block)
  }

  def foreach[A, U](from: c.Expr[Int], until: c.Expr[Int], rows: c.Expr[Int => Int], abortOnNM: c.Expr[Boolean])(f: c.Expr[(Int, A) => U]): c.Expr[Boolean] = {
    val col = freshTermName(c, "foreach$col$")
    val value = freshTermName(c, "foreach$value$")
    val row = freshTermName(c, "foreach$row$")
    val nm = freshTermName(c, "foreach$nm$")
    val i = freshTermName(c, "foreach$i$")
    val (getRow, stmts0) = sanitize(rows)
    val (iter, stmts1) = sanitize(f)

    val tree = q"""{
      var $nm = false
      ${c.prefix} match {
        case ($col: _root_.framian.UnboxedColumn[_]) =>
          var $i = $from
          while ($i < $until && !$nm) {
            val $row = $getRow($i)
            if ($col.isValueAt($row)) {
              $iter($i, $col.valueAt($row))
            } else if ($col.nonValueAt($row) == _root_.framian.NM) {
              $nm = $abortOnNM
            }
            $i += 1
          }

        case $col =>
          var $i = $from
          while ($i < $until && !$nm) {
            val $row = $getRow($i)
            $col($row) match {
              case _root_.framian.Value($value) => $iter($i, $value)
              case _root_.framian.NM => $nm = $abortOnNM
              case _root_.framian.NA =>
            }
            $i += 1
          }
      }
      !$nm
    }
    """

    val block = Block(stmts0 ++ stmts1, tree)
    inliner.inlineAndReset[Boolean](block)
  }
}

object ColumnMacros {
  def foldRowImpl[A, B](c: Context)(row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Expr[B] =
    new ColumnMacros[c.type](c).foldRow(row)(na, nm, f)

  def foreachImpl[A, U](c: Context)(from: c.Expr[Int], until: c.Expr[Int], rows: c.Expr[Int => Int])(f: c.Expr[(Int, A) => U]): c.Expr[Boolean] = {
    import c.universe._
    new ColumnMacros[c.type](c).foreach(from, until, rows, c.Expr[Boolean](q"true"))(f)
  }

  def foreachExtraImpl[A, U](c: Context)(from: c.Expr[Int], until: c.Expr[Int], rows: c.Expr[Int => Int], abortOnNM: c.Expr[Boolean])(f: c.Expr[(Int, A) => U]): c.Expr[Boolean] =
    new ColumnMacros[c.type](c).foreach(from, until, rows, abortOnNM)(f)
}
