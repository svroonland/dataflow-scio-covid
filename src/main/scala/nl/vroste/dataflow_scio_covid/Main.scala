package nl.vroste.dataflow_scio_covid

import com.spotify.scio._
import com.spotify.scio.extra.csv._
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.twitter.algebird.{
  Aggregator,
  AveragedValue,
  Moments,
  MonoidAggregator
}
import nl.vroste.dataflow_scio_covid.Util.dateToInstant
import org.apache.beam.sdk.transforms.windowing.{
  AfterWatermark,
  TimestampCombiner
}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import scala.concurrent.duration._
import scala.language.implicitConversions

object Main {
  import CsvCodecs._
  import Util.scalaDurationAsJodaDuration

  val momentsAggregator: MonoidAggregator[
    Counts,
    ((Moments, Moments), Moments),
    (Counts, Counts)
  ] =
    (Moments.aggregator zip Moments.aggregator zip Moments.aggregator)
      .composePrepare[Counts](c =>
        ((c.positiveTests, c.hospitalAdmissions), c.deaths)
      )
      .andThenPresent {
        case ((p, h), d) =>
          (
            Counts(p.mean.toInt, h.mean.toInt, d.mean.toInt),
            Counts(p.stddev.toInt, h.stddev.toInt, d.stddev.toInt)
          )
      }

  // Aggregator for whole GemeenteData objects
  val municipalityDataAggregator =
    (Aggregator.maxBy((_: MunicipalityData).date) join
      momentsAggregator.composePrepare[MunicipalityData](_.counts))
      .andThenPresent {
        case (municipalityData, (average, stddev)) =>
          CovidStatistics(
            municipalityData,
            municipalityData.counts,
            average,
            stddev
          )
      }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputPath = args("input")
    val outputPath = args("output")

    val rows: SCollection[RivmDataRow] = sc.csvFile[RivmDataRow](inputPath)

    rows
      .timestampBy(r => dateToInstant(r.date))
      .withSlidingWindows(
        size = 7.days,
        period = 1.day,
        options = WindowOptions(
          // TODO figure out which of these are essential
          timestampCombiner = TimestampCombiner.END_OF_WINDOW,
          trigger = AfterWatermark.pastEndOfWindow(),
          allowedLateness = 0.days,
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
        )
      )
      .map(MunicipalityData.fromRow)
      .keyBy(d => (d.municipality, d.date))
      .reduceByKey(MunicipalityData.add)
      .values
      .keyBy(_.municipality)
      .aggregateByKey(municipalityDataAggregator)
      .values
      .saveAsCsvFile(outputPath)

    sc.run().waitUntilFinish()
  }
}
