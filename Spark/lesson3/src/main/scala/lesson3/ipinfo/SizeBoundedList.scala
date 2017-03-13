package lesson3.ipinfo

import scala.collection._
import scala.collection.mutable.ListBuffer

//TODO use Long as index
class SizeBoundedList[A](max: Long) extends Traversable[A] with Serializable {

  val list: ListBuffer[A] = ListBuffer()

  def append(elem: A) {
    if (list.size == max) {
      list.trimStart(1)
    }
    list.append(elem)
  }

  def apply(index: Long): A = list(index.toInt)

  def foreach[U](f: A => U) = list.foreach(f)

}