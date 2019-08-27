package com.viewtreefull.utils.common.date

import java.time.LocalDate
import java.time.format.DateTimeFormatter

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

}
