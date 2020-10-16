package nl.vroste.dataflow_scio_covid

import org.joda.time.LocalDate

sealed trait DataType
case object HospitalAdmissions extends DataType
case object PositiveTests extends DataType
case object Deaths extends DataType

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
      case (PositiveTests, Some(number))      => Counts(number, 0, 0)
      case (HospitalAdmissions, Some(number)) => Counts(0, number, 0)
      case (Deaths, Some(number))             => Counts(0, 0, number)
      case _                                  => Counts(0, 0, 0)
    }
}

case class CovidStatistics(
    municipality: MunicipalityData,
    current: Counts,
    average: Counts,
    stdDev: Counts
)
