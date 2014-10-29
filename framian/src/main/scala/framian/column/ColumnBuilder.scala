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

package framian
package column

import framian.Cell

import scala.reflect.ClassTag

import scala.collection.mutable.Builder

trait ColumnBuilder[@specialized(Int,Long,Double) A] extends Builder[Cell[A], Column[A]] {
  def addValue(a: A): this.type
  def addNA(): this.type
  def addNM(): this.type
  def add(cell: Cell[A]): this.type
  def +=(cell: Cell[A]): this.type = add(cell)
  def result(): Column[A]
  def clear(): Unit
}

object ColumnBuilder {
  final def apply[A]()(implicit gen: GenColumnBuilder[A]): ColumnBuilder[A] = gen()
}

trait GenColumnBuilder[A] {
  def apply(): ColumnBuilder[A]
}

trait GenColumnBuilderLow {
  implicit def anyColumnBuilder[A]: GenColumnBuilder[A] =
    new GenColumnBuilder[A] {
      def apply() = new AnyColumnBuilder[A]
    }
}

object GenColumnBuilder extends GenColumnBuilderLow {
  implicit def genericColumnBuilder[A: ClassTag]: GenColumnBuilder[A] =
    new GenColumnBuilder[A] {
      def apply() = new GenericColumnBuilder[A]
    }

  implicit def intColumnBuilder: GenColumnBuilder[Int] =
    new GenColumnBuilder[Int] {
      def apply() = new IntColumnBuilder
    }

  implicit def longColumnBuilder: GenColumnBuilder[Long] =
    new GenColumnBuilder[Long] {
      def apply() = new LongColumnBuilder
    }

  implicit def doubleColumnBuilder: GenColumnBuilder[Double] =
    new GenColumnBuilder[Double] {
      def apply() = new DoubleColumnBuilder
    }
}

// Note: See project/Boilerplate.scala for ColumnBuilder implementations.
