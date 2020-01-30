package com.bushpath.nah.spark.sql.util

import java.util.regex.Pattern

object Parser {
    val hdfsUrlPattern = 
      Pattern.compile("hdfs://(\\d*\\.\\d*\\.\\d*\\.\\d*):(\\d*)(.*)")

    def parseHdfsUrl(url: String): (String, Int, String) = {
      val matcher = hdfsUrlPattern.matcher(url)
      matcher.find()

      (matcher.group(1), matcher.group(2).toInt, matcher.group(3))
    }
}
