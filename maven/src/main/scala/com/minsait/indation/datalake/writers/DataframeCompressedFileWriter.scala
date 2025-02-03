package com.minsait.indation.datalake.writers

import com.minsait.common.configuration.compression.FileFormatCompressionConfigurations
import com.minsait.common.utils.FileNameUtils
import com.minsait.common.utils.fs.FSCommands
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.DatasetTypes
import com.minsait.indation.metadata.models.enums.FileFormatTypes.{Csv, Xls}
import org.apache.spark.sql.{DataFrame, SaveMode}

private[datalake] object DataframeCompressedFileWriter {

  def write(fileInputPath: String, prefixTargetPath: String, df: DataFrame, ds: Dataset, fs: FSCommands, header: Boolean=false): String = {
    val fileName = if(ds.typ == DatasetTypes.File && ds.fileInput.get.format == Xls){
      fileInputPath.split("/").last.split("\\.")(0) + ".csv"
    } else {
      fileInputPath.split("/").last
    }

    val fileFormatType = if (ds.typ == DatasetTypes.File && ds.fileInput.get.format != Xls) ds.fileInput.get.format else Csv //TODO: Validar funcionamiento con unit tests

    val compressionConfig = FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(fileFormatType)

    val fileNameWithCompression = FileNameUtils.fileNameWithCompression(fileName)
    val destinationPath = prefixTargetPath + fileNameWithCompression

    fs.renameFileIfExist(destinationPath)

    // Function to convert binary columns to base64 string
    def convertBinaryColumns(df: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions.base64
      import org.apache.spark.sql.types.BinaryType
      
      df.schema.fields.foldLeft(df) { (currentDF, field) =>
        field.dataType match {
          case BinaryType => currentDF.withColumn(field.name, base64(currentDF(field.name)))
          case _ => currentDF
        }
      }
    }
    // Convert binary columns to string
    val modifiedDF = convertBinaryColumns(df)

    modifiedDF.coalesce(1).write
      .format(fileFormatType.value)
      .mode(SaveMode.Append)
      .option("compression", compressionConfig.codec.codec)
      .option("charset", "UTF-8")
      .option("header", header)
      .save(destinationPath)

    fs.moveSparkWrittenFile(destinationPath, fileNameWithCompression)

    destinationPath
  }
}
