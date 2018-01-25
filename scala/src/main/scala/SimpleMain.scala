import scala.xml.XML

object SimpleMain {
  def main(args: Array[String]): Unit = {
    val x = XML.load("https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=FullXml&term=SRX000274")
    println( ( x \\ "RUN").length )

  }
}
