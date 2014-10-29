package framian
package macroutil

import scala.language.experimental.macros

object compat {
  // Whitebox is required by Spire, but I don't think Spire needs it either.
  type Context = scala.reflect.macros.whitebox.Context

  def freshTermName(c: Context, prefix: String): c.universe.TermName =
    c.universe.TermName(c.freshName(prefix))
}
