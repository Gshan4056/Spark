package scala.base

/**
  * Created by 91926 on 2018/7/28.
  */
object setDemo {
  def main(args: Array[String]): Unit = {
    case class Book(name:String,pageNum:Int)
    val bookList = Seq(
      Book("the first one",100),
      Book("the second one",200),
      Book("the third one",300))
    println(bookList.maxBy(book => book.pageNum))
    val data = Seq(1,2,3,4,5)
    val (trainData,testData) = data.partition(x => x % 2 == 0)
    println(trainData)
    val abcd = Seq('a', 'b', 'c', 'd')
    val efgj = Seq('e', 'f', 'g', 'h')
    val ijkl = Seq('i', 'j', 'k', 'l')
    val mnop = Seq('m', 'n', 'o', 'p')
    val qrst = Seq('q', 'r', 's', 't')
    val uvwx = Seq('u', 'v', 'w', 'x')
    val yz = Seq('y', 'z')
    val alphabet = Seq(abcd, efgj, ijkl, mnop, qrst, uvwx, yz)// List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)
    println( alphabet.flatten)
    val num1 = Seq(1,2,3,4,5)
    val num2 = Seq(4,5,6,7,8)
    println(num1.diff(num2))
    println(num1.union(num2).distinct)
    println(num1.intersect(num2))
  }

}
