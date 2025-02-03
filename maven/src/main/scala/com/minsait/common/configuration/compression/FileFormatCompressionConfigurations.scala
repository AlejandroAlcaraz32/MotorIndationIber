package com.minsait.common.configuration.compression

import com.minsait.indation.metadata.models.enums.FileFormatTypes

object FileFormatCompressionConfigurations {
  sealed case class CompresionCodec(codec: String)
  object Bzip2Codec extends CompresionCodec("bzip2")
  object ZlibCodec extends CompresionCodec("zlib")
  object GzipCodec extends CompresionCodec("gzip")

  private val Bzip2: (CompresionCodec, String) = (Bzip2Codec, "bz2")
  private val Zlib: (CompresionCodec, String) = (ZlibCodec, "zlib")
  private val Gzip: (CompresionCodec, String) = (GzipCodec, "gz")

  sealed case class FileFormatCompresionConfiguration(codec: CompresionCodec, extension: String)
  object CsvCompresionConfiguration extends FileFormatCompresionConfiguration(Bzip2._1, Bzip2._2)
  object JsonCompresionConfiguration extends FileFormatCompresionConfiguration(Bzip2._1, Bzip2._2)
  object FixedCompresionConfiguration extends FileFormatCompresionConfiguration(Bzip2._1, Bzip2._2)
  object TextCompresionConfiguration extends FileFormatCompresionConfiguration(Bzip2._1, Bzip2._2)
  object OrcCompresionConfiguration extends FileFormatCompresionConfiguration(Zlib._1, Zlib._2)
  object ParquetCompresionConfiguration extends FileFormatCompresionConfiguration(Gzip._1, Gzip._2)
  object AvroCompresionConfiguration extends FileFormatCompresionConfiguration(Bzip2._1, Bzip2._2)

  def fileFormatCompressionConfiguration(fileFormat: FileFormatTypes.FileFormatType): FileFormatCompresionConfiguration = {
    fileFormat match {
      case FileFormatTypes.Csv => CsvCompresionConfiguration
      case FileFormatTypes.Json => JsonCompresionConfiguration
      case FileFormatTypes.Fixed => FixedCompresionConfiguration
      case FileFormatTypes.Text => TextCompresionConfiguration
      case FileFormatTypes.Orc => OrcCompresionConfiguration
      case FileFormatTypes.Parquet => ParquetCompresionConfiguration
      case FileFormatTypes.Avro => AvroCompresionConfiguration
      case _ => throw new UnsupportedOperationException("Unsupported FileFormat " + fileFormat.value)
    }
  }
}
