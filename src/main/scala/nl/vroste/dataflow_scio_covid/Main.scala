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
  val gemeenteDataAggregator: Aggregator[
    GemeenteData,
    (GemeenteData, ((AveragedValue, AveragedValue), AveragedValue)),
    CovidStatistics
  ] =
    (Aggregator.maxBy((_: GemeenteData).datum) join
      countAverageAggregator.composePrepare[GemeenteData](_.counts))
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
      .keyBy(_.gemeente)
      .aggregateByKey(gemeenteDataAggregator)
      .values
      .saveAsCsvFile(outputPath)

    sc.run().waitUntilFinish()
  }
}
