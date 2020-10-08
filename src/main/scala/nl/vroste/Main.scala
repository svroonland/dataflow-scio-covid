package nl.vroste

import java.time.ZoneId
import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.extra.csv._
import com.spotify.scio.values.SCollection
import com.twitter.algebird.{Aggregator, AveragedValue, MonoidAggregator}
import kantan.csv._
import org.joda.time.{DateTimeZone, Instant, LocalDate, LocalTime}

import scala.concurrent.duration._

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
      counts: Counts
  )

  case class Counts(positiveTests: Int, hospitalAdmissions: Int, deaths: Int)

  object Counts {
    def add(x: Counts, y: Counts): Counts =
      Counts(
        positiveTests = x.positiveTests + y.positiveTests,
        hospitalAdmissions = x.hospitalAdmissions + y.hospitalAdmissions,
        deaths = x.deaths + y.deaths
      )

    def fromRow(r: RivmDataRow) =
      (r.`type`, r.aantal) match {
        case (Totaal, Some(aantal))           => Counts(aantal, 0, 0)
        case (ZiekenhuisOpname, Some(aantal)) => Counts(0, aantal, 0)
        case (Overleden, Some(aantal))        => Counts(0, 0, aantal)
        case _                                => Counts(0, 0, 0)
      }
  }

  object GemeenteData {
    def fromRow(r: RivmDataRow): GemeenteData =
      GemeenteData(
        r.datum,
        r.gemeenteCode,
        r.gemeenteNaam,
        r.provincieNaam,
        r.provincieCode,
        Counts.fromRow(r)
      )

    def add(d1: GemeenteData, d2: GemeenteData): GemeenteData =
      d1.copy(counts = Counts.add(d1.counts, d2.counts))
  }

  case class CovidStatistics(
      gemeente: GemeenteData,
      current: Counts,
      average: Counts
  )
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

  implicit val encoder: HeaderEncoder[CovidStatistics] = HeaderEncoder.encoder(
    "Datum",
    "Gemeentecode",
    "Gemeentenaam",
    "Provincienaam",
    "Provinciecode",
    "HospitalAdmissions",
    "HospitalAdmissionsAvg",
    "Cases",
    "CasesAvg",
    "Deaths",
    "DeathsAvg"
  ) { (stats: CovidStatistics) =>
    (
      stats.gemeente.datum,
      stats.gemeente.gemeenteCode,
      stats.gemeente.gemeenteNaam,
      stats.gemeente.provincieNaam,
      stats.gemeente.provincieCode,
      stats.current.hospitalAdmissions,
      stats.average.hospitalAdmissions,
      stats.current.positiveTests,
      stats.average.positiveTests,
      stats.current.deaths,
      stats.average.deaths
    )
  }
}

object Main {
  import CsvDecoders._
  import Model._

  def dateToInstant(date: String): Instant =
    LocalDate
      .parse(date)
      .toDateTime(LocalTime.MIDNIGHT)
      .toDateTime(
        DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("GMT+2")))
      )
      .toInstant

  implicit def scalaDurationAsJodaDuration(
      d: FiniteDuration
  ): org.joda.time.Duration =
    org.joda.time.Duration.millis(d.toMillis)

  val averageAggregator: MonoidAggregator[
    Counts,
    ((AveragedValue, AveragedValue), AveragedValue),
    Counts
  ] =
    (AveragedValue
      .numericAggregator[Int] zip AveragedValue
      .numericAggregator[Int] zip AveragedValue.numericAggregator[Int])
      .composePrepare[Counts](c =>
        ((c.positiveTests, c.hospitalAdmissions), c.deaths)
      )
      .andThenPresent {
        case ((p, h), d) => Counts(p.toInt, h.toInt, d.toInt)
      }

  /**
    * This is an aggregator that takes the last GemeenteData and averages the three statistics
    */
  val aggregator: Aggregator[
    GemeenteData,
    (GemeenteData, ((AveragedValue, AveragedValue), AveragedValue)),
    CovidStatistics
  ] =
    (Aggregator
      .last[GemeenteData] join averageAggregator.composePrepare[GemeenteData](
      _.counts
    )).andThenPresent {
      case (gemeenteData, average) =>
        CovidStatistics(gemeenteData, gemeenteData.counts, average)
    }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputPath = args("input")
    val outputPath = args("output")

    val rows: SCollection[RivmDataRow] = sc.csvFile[RivmDataRow](inputPath)

    rows
      .timestampBy(r => dateToInstant(r.datum))
      .withSlidingWindows(size = 7.days, period = 1.day)
      .map(GemeenteData.fromRow)
      .aggregate(aggregator)
      .saveAsCsvFile(outputPath)

    sc.run().waitUntilFinish()
  }
}
