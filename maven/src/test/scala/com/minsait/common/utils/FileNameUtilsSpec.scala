package com.minsait.common.utils

import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, FileInput, SchemaColumns}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class FileNameUtilsSpec extends AnyFunSuite {

  test("FileNameUtilsSpec#fileNameWithCompression csv") {
    assert("file1.another.csv.bz2" == FileNameUtils.fileNameWithCompression("file1.another.csv"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression csv.gz") {
    assert("file1.another.csv.bz2" == FileNameUtils.fileNameWithCompression("file1.another.csv.gz"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression json") {
    assert("file1.another.json.bz2" == FileNameUtils.fileNameWithCompression("file1.another.json"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression text") {
    assert("file1.another.txt.bz2" == FileNameUtils.fileNameWithCompression("file1.another.txt"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression orc") {
    assert("file1.another.zlib.orc" == FileNameUtils.fileNameWithCompression("file1.another.orc"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression parquet") {
    assert("file1.another.gz.parquet" == FileNameUtils.fileNameWithCompression("file1.another.parquet"))
  }

  test("FileNameUtilsSpec#fileNameWithCompression avro") {
    assert("file1.another.bz2.avro" == FileNameUtils.fileNameWithCompression("file1.another.avro"))
  }

  test("FileNameUtilsSpec#landingDestinationPath") {
    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val landingDestinationPath = FileNameUtils.landingDestinationPath(
      "20200722_file.csv",
      "/tmp/landing/",
      "invalid",
      testDataset
    )

    assert("/tmp/landing/invalid/source1/dataset1/2020/07/22/" == landingDestinationPath)
  }

  test("FileNameUtilsSpec#fileExtension 1") {
    assert(".bz2.avro" == FileNameUtils.fileExtension("file1.another.bz2.avro"))
  }

  test("FileNameUtilsSpec#fileExtension 2") {
    assert(".bz2.avro" == FileNameUtils.fileExtension("/test/dir1/dir2/file1.another.bz2.avro"))
  }

  test("FileNameUtilsSpec#fileExtension 3") {
    assert(".bz2.avro" == FileNameUtils.fileExtension("abfss://subdomain.domain.com/test/dir1/dir2/file1.another.bz2.avro"))
  }

  test("FileNameUtilsSpec#fileExtension 4") {
    assert(FileNameUtils.fileExtension("abfss://subdomain.domain.com/test/dir1/dir2/file1.txt") == ".txt")
  }

  test("FileNameUtilsSpec#addTimestampToFileName 1") {
    assert(FileNameUtils.addTimestampToFileName("file1.another.bz2.avro", "yyyyMMdd_HHmm") == "file1.another_vyyyyMMdd_HHmm.bz2.avro")
  }

  test("FileNameUtilsSpec#addTimestampToFileName 2") {
    assert(FileNameUtils.addTimestampToFileName("/test/dir1/file1.another.bz2.avro", "yyyyMMdd_HHmm") == "/test/dir1/file1.another_vyyyyMMdd_HHmm.bz2.avro")
  }

  test("FileNameUtilsSpec#addTimestampToFileName 3") {
    assert(FileNameUtils.addTimestampToFileName(
      "abfss://subdomain.domain.com/test/dir1/dir2/file1.another.bz2.avro", "yyyyMMdd_HHmm") ==
      "abfss://subdomain.domain.com/test/dir1/dir2/file1.another_vyyyyMMdd_HHmm.bz2.avro")
  }
}
