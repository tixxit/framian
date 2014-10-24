package framian
package column

import scala.language.experimental.macros
import scala.reflect.macros.Context // blackbox.Context

import spire.macros.{ SyntaxUtil, InlineUtil }

class ColumnMacros[C <: Context](val c: C) {
  import c.universe._

  val util = new SyntaxUtil[c.type](c)
  val inliner = new InlineUtil[c.type](c)

  private def sanitize[A](e: c.Expr[A]): (c.Tree, List[c.Tree]) =
    if (util.isClean(e)) {
      (e.tree, Nil)
    } else {
      val name = TermName(c.freshName("norm$"))
      (q"name", List(q"val $name = $e"))
    }

  def foldRow[A, B](row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Expr[B] = {
    val col = TermName(c.freshName("foldRow$col$"))
    val value = TermName(c.freshName("foldRow$value$"))
    val r = TermName(c.freshName("foldRow$row$"))
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
    val col = TermName(c.freshName("foreach$col$"))
    val value = TermName(c.freshName("foreach$value$"))
    val row = TermName(c.freshName("foreach$row$"))
    val nm = TermName(c.freshName("foreach$nm$"))
    val i = TermName(c.freshName("foreach$i$"))
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
