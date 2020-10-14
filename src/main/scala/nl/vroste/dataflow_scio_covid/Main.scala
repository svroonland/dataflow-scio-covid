package nl.vroste.dataflow_scio_covid

import com.spotify.scio._
import com.spotify.scio.extra.csv._
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.twitter.algebird.{Aggregator, AveragedValue, MonoidAggregator}
import Util.dateToInstant
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

  // Aggregator for averages of Counts
  val countAverageAggregator: MonoidAggregator[
    Counts,
    ((AveragedValue, AveragedValue), AveragedValue),
    Counts
  ] =
    (AveragedValue.numericAggregator[Int] zip
      AveragedValue.numericAggregator[Int] zip
      AveragedValue.numericAggregator[Int])
      .composePrepare[Counts](c =>
        ((c.positiveTests, c.hospitalAdmissions), c.deaths)
      )
      .andThenPresent {
        case ((p, h), d) => Counts(p.toInt, h.toInt, d.toInt)
      }

  // Aggregator for whole GemeenteData objects
  val gemeenteDataAggregator =
    (Aggregator.maxBy((_: MunicipalityData).date) join
      countAverageAggregator.composePrepare[MunicipalityData](_.counts))
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
      .timestampBy(r => dateToInstant(r.date))
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
      .map(
        MunicipalityData.fromRow
      )
      .keyBy(d => (d.municipality, d.date))
      .reduceByKey(MunicipalityData.add)
      .values
      .keyBy(_.municipality)
      .aggregateByKey(gemeenteDataAggregator)
      .values
      .saveAsCsvFile(outputPath)

    sc.run().waitUntilFinish()
  }
}
