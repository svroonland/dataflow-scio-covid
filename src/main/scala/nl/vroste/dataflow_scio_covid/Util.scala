package nl.vroste.dataflow_scio_covid

import java.time.ZoneId
import java.util.TimeZone

import org.joda.time.{DateTimeZone, Instant, LocalDate, LocalTime}

import scala.concurrent.duration.FiniteDuration

object Util {
  def dateToInstant(date: LocalDate): Instant =
    date
      .toLocalDateTime(LocalTime.MIDNIGHT)
      .toDateTime(
        DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("GMT+2")))
      )
      .toInstant

  implicit def scalaDurationAsJodaDuration(
      d: FiniteDuration
  ): org.joda.time.Duration =
    org.joda.time.Duration.millis(d.toMillis)
}
