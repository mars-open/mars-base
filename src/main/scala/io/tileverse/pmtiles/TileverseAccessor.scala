package io.tileverse.pmtiles

import ch.zzeekk.mars.pp.TileData
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.types.StructField

import java.nio.file.Paths

object TileverseAccessor extends SmartDataLakeLogger {

  def writeTiles(
                  tiles: Iterator[TileData], localPath: String, zooms: Seq[Int], fnLog: Double => Unit,
                  layers: Map[String, Seq[StructField]],
                  compressTiles: Boolean
                ): Int = {

    val vectorLayers = layers.map{
      case (id, schema) =>
        val fields = schema
          .map(f => s""""${f.name}": "${f.getComment().map(_.replaceAll("[\n\r]", ". ")).getOrElse(f.name)}"""")
          .mkString(", ")
        s"""{"id": "$id", "fields": {$fields}}"""
    }
    val metadata = s"""{
      "vector_layers": [
        ${vectorLayers.mkString(","+System.lineSeparator())}
      ]
    }"""

    val writer = PMTilesWriter.builder()
      .outputPath(Paths.get(localPath))
      .maxZoom(zooms.max)
      .minZoom(zooms.min)
      .tileType(PMTilesHeader.TILETYPE_MVT)
      .internalCompression(PMTilesHeader.COMPRESSION_GZIP)
      .tileCompression(if (compressTiles) PMTilesHeader.COMPRESSION_GZIP else PMTilesHeader.COMPRESSION_NONE)
      .center(46.95112222715324, 7.439325799911505, 15)
      .build()

    writer.setMetadata(metadata)

    writer.setProgressListener(createProgressListener(fnLog))

    var i = 0
    tiles.foreach {
      tileData =>
        i += 1
        writer.addTile(tileData.tile.getIndex, tileData.data)
    }
    writer.complete()
    i
  }

  def compressUsingGzip(data: Array[Byte]): Array[Byte] = CompressionUtil.compress(data, PMTilesHeader.COMPRESSION_GZIP)

  def createProgressListener(fnLog: Double => Unit): PMTilesWriter.ProgressListener = {
    new PMTilesWriter.ProgressListener() {
      private var lastProgress = 0
      override def onProgress(progress: Double): Unit = {
        val roundProgress = math.floor(progress * 100).toInt
        if (lastProgress < roundProgress) fnLog(roundProgress)
        lastProgress = roundProgress
      }
      override def isCancelled: Boolean = false
    }
  }
}
