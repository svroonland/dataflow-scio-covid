package nl.vroste.dataflow_scio_covid

import org.joda.time.LocalDate

sealed trait DataType
case object ZiekenhuisOpname extends DataType
case object Totaal extends DataType
case object Overleden extends DataType

case class RivmDataRow(
    date: LocalDate,
    municipality: String,
    province: String,
    `type`: DataType,
    number: Option[Int],
    numberCumulative: Option[Int]
)
case class MunicipalityData(
    date: LocalDate,
    municipality: String,
    province: String,
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

  def fromRow(r: RivmDataRow): Counts =
    (r.`type`, r.number) match {
      case (Totaal, Some(aantal))           => Counts(aantal, 0, 0)
      case (ZiekenhuisOpname, Some(aantal)) => Counts(0, aantal, 0)
      case (Overleden, Some(aantal))        => Counts(0, 0, aantal)
      case _                                => Counts(0, 0, 0)
    }
}

object MunicipalityData {
  def fromRow(r: RivmDataRow): MunicipalityData =
    MunicipalityData(
      r.date,
      r.municipality,
      r.province,
      Counts.fromRow(r)
    )

  def add(d1: MunicipalityData, d2: MunicipalityData): MunicipalityData =
    d1.copy(counts = Counts.add(d1.counts, d2.counts))
}

case class CovidStatistics(
    municipality: MunicipalityData,
    current: Counts,
    average: Counts
)
