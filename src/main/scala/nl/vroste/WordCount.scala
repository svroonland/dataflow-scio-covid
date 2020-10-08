package nl.vroste

import com.spotify.scio._
import kantan.csv._
import com.spotify.scio.extra.csv._
import com.spotify.scio.extra.sorter._
import com.spotify.scio.values.SCollection

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
 */

object Model {
  sealed trait DataType
  case object ZiekenhuisOpname extends DataType
  case object Totaal extends DataType
  case object Overleden extends DataType

  case class RivmDataRow(
      datum: String,
      gemeenteCode: Int,
      gemeenteNaam: String,
      provincieNaam: String,
      provincieCode: Int,
      `type`: DataType,
      aantal: Option[Int],
      aantalCumulatief: Option[Int]
  )
  case class GemeenteData(
      datum: String,
      gemeenteCode: Int,
      gemeenteNaam: String,
      provincieNaam: String,
      provincieCode: Int,
      ziekenhuisOpnames: Int,
      totaal: Int,
      overleden: Int
  )

  object GemeenteData {
    def fromRow(r: RivmDataRow): GemeenteData = {
      val (ziekenhuisOpnames, totaal, overleden) = (r.`type`, r.aantal) match {
        case (ZiekenhuisOpname, Some(aantal)) => (aantal, 0, 0)
        case (Totaal, Some(aantal))           => (0, aantal, 0)
        case (Overleden, Some(aantal))        => (0, 0, aantal)
        case _                                => (0, 0, 0)
      }

      GemeenteData(
        r.datum,
        r.gemeenteCode,
        r.gemeenteNaam,
        r.provincieNaam,
        r.provincieCode,
        ziekenhuisOpnames,
        totaal,
        overleden
      )
    }

    def add(d1: GemeenteData, d2: GemeenteData): GemeenteData =
      d1.copy(
        ziekenhuisOpnames = d1.ziekenhuisOpnames + d2.ziekenhuisOpnames,
        totaal = d1.ziekenhuisOpnames + d2.ziekenhuisOpnames,
        overleden = d1.overleden + d2.overleden
      )

    def fromRows(rows: Seq[RivmDataRow]): Option[GemeenteData] =
      for {
        r <- rows.headOption
        ziekenhuisOpnameRow <- rows.find(_.`type` == ZiekenhuisOpname)
        totaalRow <- rows.find(_.`type` == Totaal)
        overledenRow <- rows.find(_.`type` == Overleden)
      } yield GemeenteData(
        r.datum,
        r.gemeenteCode,
        r.gemeenteNaam,
        r.provincieNaam,
        r.provincieCode,
        ziekenhuisOpnameRow.aantal.getOrElse(0),
        totaalRow.aantal.getOrElse(0),
        overledenRow.aantal.getOrElse(0)
      )
  }
}

object CsvDecoders {
  import Model._

  implicit val dataTypeDecoder: CellDecoder[Model.DataType] =
    CellDecoder[String].emap {
      case "Totaal"           => Right(Totaal)
      case "Ziekenhuisopname" => Right(ZiekenhuisOpname)
      case "Overleden"        => Right(Overleden)
    }

  implicit val decoder: HeaderDecoder[RivmDataRow] = HeaderDecoder.decoder(
    "Datum",
    "Gemeentecode",
    "Gemeentenaam",
    "Provincienaam",
    "Provinciecode",
    "Type",
    "Aantal",
    "AantalCumulatief"
  )(RivmDataRow.apply)
}

object WordCount {
  import Model._
  import CsvDecoders._

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val exampleData = "gs://steven-dataworkz-scio/RIVM_NL_municipal.csv"
    val input = args.getOrElse("input", exampleData)
    val output = args("output")

    val rows: SCollection[RivmDataRow] = sc.csvFile(input)

    rows
      .map(r => ((r.datum, r.gemeenteCode), GemeenteData.fromRow(r)))
      .reduceByKey(GemeenteData.add)
      .map(_._2)
      .saveAsTextFile(output)

    sc.run().waitUntilFinish()
  }
}
