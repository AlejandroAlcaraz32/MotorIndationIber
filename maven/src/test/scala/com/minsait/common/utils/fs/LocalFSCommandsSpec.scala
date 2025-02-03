package com.minsait.common.utils.fs

import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class LocalFSCommandsSpec extends AnyFunSuite {
  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\","/") + "/"

  test("LocalFSCommandsSpec#directoryTree") {
      val expectDirs = List(
        testResourcesBasePath + "bronze/landing/pending/source1/",
        testResourcesBasePath + "bronze/landing/pending/source2/",
        testResourcesBasePath + "bronze/landing/pending/source3/",
        testResourcesBasePath + "bronze/landing/pending/source4/",
        testResourcesBasePath + "bronze/landing/pending/source5/",
        testResourcesBasePath + "bronze/landing/pending/source6/",
        testResourcesBasePath + "bronze/landing/pending/source7/",
        testResourcesBasePath + "bronze/landing/pending/source8/"
      )
      val dirs = LocalFSCommands.directoryTree(testResourcesBasePath + "bronze/landing/pending/")
      assert(dirs.sorted.map(_.replace("\\","/")) == expectDirs.sorted)
  }

  test("LocalFSCommandsSpec#waitForFinishedCopy"){

    val path = testResourcesBasePath + "/landing/source1/corrupted.csv"

    LocalFSCommands.waitForFinishedCopy(path)
  }

  test ("List directory"){
    val files = LocalFSCommands.ls(testResourcesBasePath + "bronze")
    println(files.mkString("\n"))
  }
}
