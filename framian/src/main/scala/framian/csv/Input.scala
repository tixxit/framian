package framian.csv

case class Input(offset: Long, data: String, isLast: Boolean, mark: Long) {
  private def check(i: Long): Int = if ((i < offset) || (i > (offset + data.length))) {
    throw new IndexOutOfBoundsException()
  } else {
    val j = i - offset
    if (j <= Int.MaxValue) {
      j.toInt
    } else {
      throw new IndexOutOfBoundsException()
    }
  }

  def charAt(i: Long): Char = data.charAt(check(i))

  def length: Long = offset + data.length

  def substring(from: Long, until: Long): String =
    data.substring(check(from), check(until))

  def marked(pos: Long): Input =
    Input(offset, data, isLast, pos)

  private def trim: Input = if (mark > offset) {
    val next = spire.math.min(mark - offset, data.length.toLong).toInt
    val tail = data.substring(next)
    val offset0 = offset + next
    Input(offset0, tail, isLast, offset0)
  } else this

  def append(chunk: String, last: Boolean = false): Input =
    if (mark > offset) trim.append(chunk, last)
    else if (chunk.isEmpty) Input(offset, data, last, mark)
    else Input(offset, data + chunk, last, mark)

  def finished: Input = Input(offset, data, true, mark)
}

object Input {
  def fromString(str: String): Input =
    Input(0, str, true, 0)

  def init(str: String): Input =
    Input(0, str, false, 0)
}
