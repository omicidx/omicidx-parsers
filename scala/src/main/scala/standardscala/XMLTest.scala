package standardscala

import scala.xml.XML

object XMLTest {
  def main(args: Array[String]): Unit = {
    val x = XML.load("https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=FullXml&term=SRX000273")
    /* iterate over nodes */
    for (n <- x.descendant) println(n.label) 
    println(( x \\ "RUN").length)
  }
}
