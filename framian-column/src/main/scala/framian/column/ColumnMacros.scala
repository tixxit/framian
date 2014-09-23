package framian
package column

import scala.language.experimental.macros
import scala.reflect.macros.Context

import spire.macros.{ SyntaxUtil, InlineUtil }

class ColumnMacros[C <: /*blackbox.*/Context](val c: C) {
  import c.universe._

  val util = new SyntaxUtil[c.type](c)
  val inliner = new InlineUtil[c.type](c)

  private def sanitize[A](e: c.Expr[A]): (c.Tree, List[c.Tree]) =
    if (util.isClean(e)) {
      (e.tree, Nil)
    } else {
      val name = newTermName(c.fresh("norm$"))
      (q"name", List(q"val $name = $e"))
    }

  def foldRow[A, B](row: c.Expr[Int])(na: c.Expr[B], nm: c.Expr[B], f: c.Expr[A => B]): c.Expr[B] = {
    val col = newTermName(c.fresh("foldRow$col$"))
    val value = newTermName(c.fresh("foldRow$value$"))
    val r = newTermName(c.fresh("foldRow$row$"))
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
    val col = newTermName(c.fresh("foreach$col$"))
    val value = newTermName(c.fresh("foreach$value$"))
    val row = newTermName(c.fresh("foreach$row$"))
    val nm = newTermName(c.fresh("foreach$nm$"))
    val i = newTermName(c.fresh("foreach$i$"))
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

