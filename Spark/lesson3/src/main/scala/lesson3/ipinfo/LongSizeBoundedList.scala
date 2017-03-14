package lesson3.ipinfo

//TODO use Long as index
class LongSizeBoundedList(max: Long) extends SizeBoundedList[Long](max: Long) {
  var sum: Long = 0

  override def append(elem: Long) {
    if (list.size == max) {
      sum -= list.head
      list.trimStart(1)
    }
    list.append(elem)
    sum += elem
  }
}