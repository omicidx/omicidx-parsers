import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML
import scala.xml.Elem

case class Run(accession: String, alias: String, run_date: String,
  title: String, experiment_accession: String, total_spots: Option[Int],
  attributes: List[Map[String,String]])

object XMLReader {
  def xmlLoader(a: String): scala.xml.Elem = {
    val x = XML.load(s"https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=FullXml&term=${a}")
    return(x)
  }

  def RunGetter2(n: scala.xml.Node): Run = {
    val accession = n.attributes.get("accession").toString
    val alias = n.attributes.get("alias").toString
    val run_date = n.attributes.get("run_date").toString
    val total_spots = n.attribute("total_spots").map( x => x.toString.toInt)
    var experiment_accession = ""
    for(node <- n.descendant) {
      if(node.label == "EXPERIMENT_REF") {
        experiment_accession = node.attribute("accession").toString
      }
    }
    return(Run(accession, alias, run_date, "", experiment_accession, total_spots, List()))
  }


  def RunGetter(n: scala.xml.Node): Run = {
    val accession = n.attributes.get("accession").toString
    val alias = n.attributes.get("alias").toString
    val run_date = n.attributes.get("run_date").toString()
    val experiment_accession = "" // todo
    return(Run(accession, alias, run_date, "", experiment_accession,Some(0), List()))
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local[*]")
      .appName("XML Reading")
      .getOrCreate()
    println("here")
    println("here")
    val sc = sparkSession.sparkContext
    // val sqlContext = new SQLContext(sc)
    val abc = sc.parallelize(Seq("SRX2896187","SRX000273","SRX2896185", "SRP108852"))
    val y = abc.map( f => (f, xmlLoader(f)))
    val z = y.flatMap( f => (f._2 \\ "RUN")).map( f => RunGetter2(f))
    println(y.count)
    z.collect.foreach(f => println(s"${f}"))
    //sparkSession.stop()
  }
}
