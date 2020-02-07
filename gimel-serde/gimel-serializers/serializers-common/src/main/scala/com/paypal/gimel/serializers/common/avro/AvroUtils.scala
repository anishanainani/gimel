/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.gimel.serializers.common.avro

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql._

import com.paypal.gimel.logger.Logger

object AvroUtils extends Serializable {

  val logger = Logger()

  /**
    * Converts a DataFrame into RDD[Avro Generic Record]
    *
    * @param dataFrame        DataFrame
    * @param avroSchemaString Avro Schema String
    * @return RDD[GenericRecord]
    */

  def dataFrametoBytes(dataFrame: DataFrame, avroSchemaString: String): DataFrame = {
    import dataFrame.sparkSession.implicits._
    try {
      if (!isDFFieldsEqualAvroFields(dataFrame, avroSchemaString)) {
        throw new Exception(s"Incompatible DataFrame Schema Vs Provided Avro Schema.")
      }
      dataFrame.map { row =>
        val avroSchema = (new Schema.Parser).parse(avroSchemaString)
        val fields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
        val cols: Map[String, Any] = row.getValuesMap(fields)
        val genericRecord: GenericRecord = new GenericData.Record(avroSchema)
        cols.foreach(x => genericRecord.put(x._1, x._2))
        genericRecordToBytes(genericRecord, avroSchemaString)
      }.toDF("value")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Compare Fields of Avro Schema with Fields of DataFrame
    * Return true if both match false if there is any mismatch
    * Also log/print the differences.
    *
    * @param dataFrame        DataFrame
    * @param avroSchemaString Avro Schema String
    * @return Boolean
    */
  def isDFFieldsEqualAvroFields(dataFrame: DataFrame, avroSchemaString: String): Boolean = {
    try {
      val dfFields = dataFrame.schema.fieldNames
      val avroSchema = (new Schema.Parser).parse(avroSchemaString)
      val avroFields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
      val inDFMissingInAvro = dfFields.diff(avroFields)
      val inAvroMissingInDF = avroFields.diff(dfFields)
      val isMatching = inDFMissingInAvro.isEmpty && inAvroMissingInDF.isEmpty
      if (!isMatching) {
        val warningMessage =
          s"""
             |Provided Avro Fields --> ${avroFields.mkString(",")}
             |Determined DataFrame Fields --> ${dfFields.mkString(",")}
             |Missing Fields in Avro --> ${inDFMissingInAvro.mkString(",")}
             |Missing Fields in DataFrame --> ${inAvroMissingInDF.mkString(",")}
          """.stripMargin
        logger.warning(warningMessage)
      }
      isMatching
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }

  }

  /**
    * Serialize Avro GenericRecord into Byte Array
    *
    * @param rec          An Avro Generic Record
    * @param schemaString An Avro Schema String
    * @return Serialized Byte Array
    */

  def genericRecordToBytes(rec: GenericRecord, schemaString: String): Array[Byte] = {

    try {
      // Build Avro Schema From String
      val avroSchema = (new Schema.Parser).parse(schemaString)
      // Initiate a new Java Byte Array Output Stream
      val out = new ByteArrayOutputStream()
      // Get appropriate AVRO Decoder from Factory
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      // Write the Encoded data's output (Byte Array) into the Output Stream
      // Initiate AVRO Writer from Factory
      val writer = new SpecificDatumWriter[GenericRecord](avroSchema)
      writer.write(rec, encoder)
      // Flushes Data to Actual Output Stream
      encoder.flush()
      // Close the Output Stream
      out.close()
      val serializedBytes: Array[Byte] = out.toByteArray
      serializedBytes
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

}
