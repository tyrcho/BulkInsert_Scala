package stats

import rx.lang.scala._
import rx.observables.StringObservable._
import rx.lang.scala.JavaConversions._

object RxDemo extends App {
  val reader = io.Source.fromFile("couverture-2g-3g-4g-en-france-par-operateur-juillet-2015.csv").reader
  val lines: Observable[Line] = byLine(from(reader))
  val lineRegexp = """(\d+);(\d+);([\s\w&-]+);(\d+);(\d+\.\d+);(\d+\.\d+);([\s]?[-]?\d+\.\d+),([\s]?[-]?\d+\.\d+);([\s\w\p{L}]+);(\d+\.\d+);([\s\w\p{L}]+);([\s\w]+);([\s\w]+);(\w+)""".r

  def parseLine(l: String) = l match {
    case lineRegexp(postal, insee, commune, dpt, surface, pop, geo_lat, geo_long, var1, couverture, operateur, var2, var3, reseau) =>
      Some(Data(postal.toInt, insee.toInt, commune, dpt.toInt, surface.toFloat, pop.toDouble.toInt, Geo(geo_lat.toDouble, geo_long.toDouble), couverture.toDouble,
        operateur, var3, reseau))
    case _ =>
      //      println("NO match" + l)
      None
  }

  val data = for {
    l <- lines
    d = parseLine(l.getText)
    if d.isDefined
  } yield d.get

  val spots = for {
    (code, obs) <- data.groupBy(_.code_INSEE).take(10)
  } {
    obs.subscribe(d => println(code -> d), e => e.printStackTrace(), () => println(code + " is finished"))
  }

}