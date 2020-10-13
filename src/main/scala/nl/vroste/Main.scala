package nl.vroste

import java.time.ZoneId
import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.extra.csv._
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.twitter.algebird.{Aggregator, AveragedValue, MonoidAggregator}
import kantan.csv._
import org.apache.beam.sdk.transforms.windowing.{
  AfterWatermark,
  TimestampCombiner
}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{DateTimeZone, Instant, LocalDate, LocalTime}

import scala.concurrent.duration._
import scala.language.implicitConversions

object Model {
  sealed trait DataType
  case object ZiekenhuisOpname extends DataType
  case object Totaal extends DataType
  case object Overleden extends DataType

  case class RivmDataRow(
      datum: LocalDate,
      gemeente: String,
      provincie: String,
      `type`: DataType,
      aantal: Option[Int],
      aantalCumulatief: Option[Int]
  )
  case class GemeenteData(
      datum: LocalDate,
      gemeente: String,
      provincie: String,
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
        r.gemeente,
        r.provincie,
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

  def dateToInstant(date: LocalDate): Instant =
    date
      .toLocalDateTime(LocalTime.MIDNIGHT)
      .toDateTime(
        DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("GMT+2")))
      )
      .toInstant

  implicit val localDateDecoder: CellDecoder[LocalDate] =
    CellDecoder[String].map(LocalDate.parse)

  implicit val localDateEncoder: CellEncoder[LocalDate] =
    CellEncoder[String].contramap[LocalDate](dateToInstant(_).toString)

  implicit val decoder: HeaderDecoder[RivmDataRow] = HeaderDecoder.decoder(
    "Datum",
    "Gemeentenaam",
    "Provincienaam",
    "Type",
    "Aantal",
    "AantalCumulatief"
  )(RivmDataRow.apply)

  implicit val encoder: HeaderEncoder[CovidStatistics] = HeaderEncoder.encoder(
    "Datum",
    "Gemeentenaam",
    "Provincienaam",
    "HospitalAdmissions",
    "HospitalAdmissionsAvg",
    "Cases",
    "CasesAvg",
    "Deaths",
    "DeathsAvg"
  ) { (stats: CovidStatistics) =>
    (
      stats.gemeente.datum,
      stats.gemeente.gemeente,
      stats.gemeente.provincie,
      stats.current.hospitalAdmissions,
      stats.average.hospitalAdmissions,
      stats.current.positiveTests,
      stats.average.positiveTests,
      stats.current.deaths,
      stats.average.deaths
    )
  }

  implicit val gemeenteDataEncoder: HeaderEncoder[GemeenteData] =
    HeaderEncoder.encoder(
      "Datum",
      "Gemeentenaam",
      "Provincienaam",
      "HospitalAdmissions",
      "Cases",
      "Deaths"
    ) { (data: GemeenteData) =>
      (
        data.datum,
        data.gemeente,
        data.provincie,
        data.counts.hospitalAdmissions,
        data.counts.positiveTests,
        data.counts.deaths
      )
    }
}

object Main {
  import CsvDecoders._
  import Model._

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
    (Aggregator.last[GemeenteData] join
      averageAggregator.composePrepare[GemeenteData](_.counts))
      .andThenPresent {
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
      .withSlidingWindows(
        size = 7.days,
        period = 1.day,
        options = WindowOptions(
          timestampCombiner = TimestampCombiner.END_OF_WINDOW,
          trigger = AfterWatermark.pastEndOfWindow(),
          allowedLateness = 0.days,
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
        )
      )
      .map(GemeenteData.fromRow)
      .keyBy(d => (d.gemeente, d.datum))
      .reduceByKey(GemeenteData.add)
      .values
      .groupMapReduce(_.gemeente)(GemeenteData.add)
      .aggregateByKey(aggregator)
      .values
      .saveAsCsvFile(outputPath)

    sc.run().waitUntilFinish()
  }
}
