package nl.vroste.dataflow_scio_covid

import org.joda.time.LocalDate

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
