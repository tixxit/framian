package framian
package macroutil

import scala.language.experimental.macros

object compat {
  type Context = scala.reflect.macros.Context

  def freshTermName(c: Context, prefix: String): c.universe.TermName =
    c.universe.newTermName(c.fresh(prefix))
}
