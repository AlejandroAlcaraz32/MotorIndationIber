package com.minsait.indation.metadata.helpers

import org.scalatest.funsuite.AnyFunSuite

class FilePatternsHelperSpec extends AnyFunSuite {

  test("FilePatternsHelperSpec#filepattern 1") {
    assert(FilePatternsHelper.fileNameMatchesFilePattern("20200101_filepattern_worldCities_20200720072016_I.csv",
      "<yyyy><mm><dd>_filepattern_worldCities_<tttttttttttttt>_I.csv" ))
  }

  test("FilePatternsHelperSpec#filepattern 2") {
    assert(FilePatternsHelper.fileNameMatchesFilePattern("20200101_filepattern_worldCities_20200720072016_I.csv",
      "<yyyy><mm><dd>_filepattern_worldCities_<tttttttttttttt>_<t>.csv" ))
  }

}
