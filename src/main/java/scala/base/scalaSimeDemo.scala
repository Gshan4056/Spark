package scala.base

/**
  * Created by 91926 on 2018/7/11.
  */
object scalaSimeDemo {
  def main(args: Array[String]): Unit = {
    case class Book(title: String, pages: Int)

    val books = Seq(
      Book("Future of Scala developers", 85),
      Book("Parallel algorithms", 240),
      Book("Object Oriented Programming", 130),
      Book("Mobile Development", 495)
    )
   val res = books.filter(book => book.pages >120)
    println(res)

  }
}
