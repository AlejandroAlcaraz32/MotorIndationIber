package com.minsait.indation.metadata.helpers

import com.minsait.common.utils.FileNameUtils

private[metadata] object FilePatternsHelper {
  def fileNameMatchesFilePattern(fileName: String, expectedPattern: String): Boolean = {
    val patterns = FileNameUtils.patterns(expectedPattern)

    val expected = patterns.map(_.element).fold(expectedPattern) {
      (acum: String, pattern: String) => acum.replace("<" + pattern + ">", "")
    }

    val indexes = patterns.map(p => p.start to p.end).flatten

    val staticFilename = fileName.toCharArray.zipWithIndex.filter(p => !indexes.contains(p._2)).map(_._1).mkString

    expected.equals(staticFilename)
  }
}
