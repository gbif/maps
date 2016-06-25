package org.gbif.maps.spark

import java.net.URL

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources

object ConfTest {
  def main(args: Array[String]): Unit = {

    val confUrl = Resources.getResource("dev.yml")

    val mapper = new ObjectMapper(new YAMLFactory())
    val config: MapConfiguration = mapper.readValue(confUrl, classOf[MapConfiguration])

    System.out.println("SRS: " + config.tilePyramid.srs(0))
  }
}
