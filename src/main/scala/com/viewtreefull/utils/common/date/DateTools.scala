package com.viewtreefull.utils.common.date

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime}

object DateTools {

  def getFormattedDate(date: LocalDate, dateFormat: String): String = {
    date.format(DateTimeFormatter.ofPattern(dateFormat))
  }

  def getNDaysAgo(n: Int, dateFormat: String): String = {
    getFormattedDate(LocalDate.now.minusDays(n), dateFormat)
  }

  def getNDaysLater(n: Int, dateFormat: String): String = {
    getFormattedDate(LocalDate.now.plusDays(n), dateFormat)
  }

  def getNDaysDiffFromNow(n: Int, dateFormat: String): String = {
    if (n > 0) getNDaysLater(n, dateFormat)
    else getNDaysAgo(math.abs(n), dateFormat)
  }

  def getToday(dateFormat: String): String = {
    getFormattedDate(LocalDate.now, dateFormat)
  }

  /**
   * calculate the difference between start and end
   *
   * @param start time before
   * @param end   time after
   * @return (hour, min, sec)
   */
  def getTimeDiff(start: LocalTime, end: LocalTime): (Long, Long, Long) = {
    var hour: Long = 0
    var min: Long = 0
    var sec: Long = 0
    var remainder: Long = 0

    val secondsBetween = ChronoUnit.SECONDS.between(start, end)
    if (secondsBetween >= 3600) {
      hour = secondsBetween / 3600
      remainder = secondsBetween % 3600
      min = remainder / 60
      sec = remainder % 60
    } else if (secondsBetween >= 60) {
      min = secondsBetween / 60
      sec = secondsBetween % 60
    } else {
      sec = secondsBetween
    }

    (hour, min, sec)
  }
}
