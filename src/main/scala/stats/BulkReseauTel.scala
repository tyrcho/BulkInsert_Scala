package stats

import java.io._
import rapture._
import core._, json._
import jsonBackends.jawn._
import formatters.humanReadable._
import scala.collection.AbstractIterator
import scala.collection.Iterator

object BulkReseauTel extends App {

  def uniqueBy[T, U](it: Iterator[T])(pred: T => U) = new AbstractIterator[T] {

    var seen = Set.empty[U]
    var n = if (it.hasNext) Some(it.next()) else None

    def hasNext = n.nonEmpty
    def next() = {
      val res = n.get
      seen += pred(res)
      n = None
      while (it.hasNext && n.isEmpty) {
        val nn = it.next()
        if (!seen.contains(pred(nn))) n = Some(nn)
      }
      res
    }
  }

  /**
   * Settings
   */

  // input file
  val lines = io.Source.fromFile("couverture-2g-3g-4g-en-59-par-operateur-juillet-2015.csv").getLines

  // output file
  val filePrefix = "_59"
  val path2Files = "./target/"
  val fileNames = List("singleRecords", "cities", "spots")

  val outputFiles = fileNames.map(e => new File(path2Files + e + filePrefix + ".js"))
  val bwNames = outputFiles.map(e => new BufferedWriter(new FileWriter(e)))

  // mongoDB collections to feed
  val collection_name = List("resTel", "communes", "spots")

  // filters
  val departements = List(59)

  val lineRegexp = """(\d+);(\d+);([\s\w&-]+);(\d+);(\d+\.\d+);(\d+\.\d+);([\s]?[-]?\d+\.\d+),([\s]?[-]?\d+\.\d+);([\s\w\p{L}]+);(\d+\.\d+);([\s\w\p{L}]+);([\s\w]+);([\s\w]+);(\w+)""".r

  def parseLine(l: String) = l match {
    case lineRegexp(postal, insee, commune, dpt, surface, pop, geo_lat, geo_long, var1, couverture, operateur, var2, var3, reseau) =>
      Some(Data(postal.toInt, insee.toInt, commune, dpt.toInt, surface.toFloat, pop.toDouble.toInt, Geo(geo_lat.toDouble, geo_long.toDouble), couverture.toDouble,
        operateur, var3, reseau))
    case _ => None
  }

  /**
   * Browsing and filtering input file
   */
  val it = for {
    l <- lines
    data <- parseLine(l)
    if departements.contains(data.code_dpt)
  } yield (data)

  val (it0, it1) = it.duplicate // it0 pour duplication, it1 pour # of lines
  val (it2, it3) = it0.duplicate // it2 pour itération copie, it3 pour set des communes

  val numberOfLines = it1.size
  println(f"# lines = ${numberOfLines}")

  /**
   * Count distinct cities
   */
  val itCommunes = for {
    j <- it3
  } yield (j.code_INSEE)
  val communes = itCommunes.toSet
  val nbCommunes = communes.size
  println(f"# distinct cities= $nbCommunes")

  /**
   * Generating output files
   */
  (bwNames zip collection_name).map { case (bwName, name) => bwName.write(f"db.${name}.insert(\r\n[\r\n") }

  var breakINSEE = -1
  var n = 0
  var listOperators: List[Operateur] = Nil

  while (it2.hasNext) {
    val elem = it2.next()
    val operator = Operateur(elem.operateur, elem.couverture, elem.reseau, elem.type_couverture)

    // accumulate operators
    listOperators = operator :: listOperators

    if (elem.code_INSEE != breakINSEE) {
      n = n + 1

      // write records except at the beginning
      if (n != 1) {
        // dump data into files
        val sep = if (n == nbCommunes) "\r\n" else ",\r\n"
        val city = Commune(elem.code_postal, elem.code_INSEE, elem.nom_commune, elem.code_dpt, elem.surface_km2, elem.population, elem.posGeo)
        val singleRecord = SingleRecord(elem.code_postal, elem.code_INSEE, elem.nom_commune, elem.code_dpt, elem.surface_km2, elem.population, elem.posGeo, operateurs = listOperators)
        val spot = Spot(elem.code_INSEE, operateurs = listOperators)

        val strData = List(Json(singleRecord).toString + sep, Json(city).toString + sep, Json(spot).toString + sep)
        (bwNames zip strData).map { case (bwName, str) => bwName.write(str) }

        listOperators = List()
        breakINSEE = elem.code_INSEE
      }
    }
  }

  bwNames.map(_.write("]\r\n)"))
  bwNames.map(_.close())

} // end object

case class Geo(latitude: Double, longitude: Double)
case class Operateur(nom_operateur: String, couverture: Double, reseau: String, type_couverture: String)
case class SingleRecord(code_postal: Int, code_INSEE: Int, nom_commune: String, code_dpt: Int, surface_km2: Float, population: Int, posGeo: Geo, operateurs: List[Operateur])
case class Commune(code_postal: Int, code_INSEE: Int, nom_commune: String, code_dpt: Int, surface_km2: Float, population: Int, posGeo: Geo)
case class Data(code_postal: Int, code_INSEE: Int, nom_commune: String, code_dpt: Int, surface_km2: Float, population: Int, posGeo: Geo, couverture: Double, operateur: String, type_couverture: String, reseau: String) {
  def op = Operateur(operateur, couverture, reseau, type_couverture)
}
case class Spot(code_INSEE: Int, operateurs: List[Operateur])

