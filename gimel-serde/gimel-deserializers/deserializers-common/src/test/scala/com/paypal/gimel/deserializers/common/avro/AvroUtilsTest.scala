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

package com.paypal.gimel.deserializers.common.avro

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.DataFrame
import org.apache.trevni.avro.RandomData
import org.scalatest._

import com.paypal.gimel.deserializers.common.SharedSparkSession

class AvroUtilsTest extends FunSpec with Matchers with SharedSparkSession {

  val avroSchema = s"""{"namespace": "namespace",
          "type": "record",
          "name": "test",
          "fields": [
              {\"name\": \"null\", \"type\": \"null\"},
              {\"name\": \"boolean\", \"type\": \"boolean\"},
              {\"name\": \"int\", \"type\": \"int\"},
              {\"name\": \"long\", \"type\": \"long\"},
              {\"name\": \"float\", \"type\": \"float\"},
              {\"name\": \"double\", \"type\": \"double\"},
              {\"name\": \"bytes\", \"type\": \"bytes\"},
              {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},
              {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},
              {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},
              {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},
              {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},
              {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},
              {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},
              {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},
              {\"name\": \"string_default\", \"type\": \"string\", \"default\": \"default string\"}
         ]
         }""".stripMargin

  val newAvroSchema = s"""{"namespace": "namespace",
          "type": "record",
          "name": "test",
          "fields": [
              {\"name\": \"null\", \"type\": \"null\"},
              {\"name\": \"boolean\", \"type\": \"boolean\"},
              {\"name\": \"int\", \"type\": \"int\"},
              {\"name\": \"long\", \"type\": \"long\"},
              {\"name\": \"float\", \"type\": \"float\"},
              {\"name\": \"double\", \"type\": \"double\"},
              {\"name\": \"bytes\", \"type\": \"bytes\"},
              {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},
              {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},
              {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},
              {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},
              {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},
              {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},
              {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},
              {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},
              {\"name\": \"string_default\", \"type\": \"string\", \"default\": \"default string\"},
              {\"name\": \"custom\", \"type\": \"string\", \"default\": \"custom value\"}
         ]
         }""".stripMargin

  val empAvroSchema =
    s"""{"namespace": "namespace",
          "type": "record",
          "name": "test_emp",
          "fields": [
              {\"name\": \"name\", \"type\": \"string\"},
              {\"name\": \"age\", \"type\": \"string\"},
              {\"name\": \"id\", \"type\": \"string\"},
              {\"name\": \"address\", \"type\": \"string\"},
              {\"name\": \"company\", \"type\": \"string\"},
              {\"name\": \"designation\", \"type\": \"string\"},
              {\"name\": \"salary\", \"type\": \"string\"}
         ]
         }""".stripMargin

  describe("getFieldsFromAvroSchemaString") {
    it ("should return a list of fields from avro schema") {
      val fields = AvroUtils.getFieldsFromAvroSchemaString(avroSchema)
      assert(fields.sameElements(
        List("null", "boolean", "int", "long", "float", "double", "bytes", "string", "null_default", "boolean_default", "int_default", "long_default", "float_default", "double_default", "bytes_default", "string_default")
      ))
    }
  }

  describe("bytesToGenericRecordWithSchemaRecon") {
    it ("should convert avro bytes to GenericRecord") {
      val (recordBytes, genericRecord) = createRandomAvroRecordBytes(avroSchema)
      AvroUtils.bytesToGenericRecordWithSchemaRecon(recordBytes, avroSchema, avroSchema)
        .shouldBe(genericRecord)
    }
  }

  describe("copyToGenericRecord") {
    it ("should copy given generic record to new generic record with new schema") {
      val genericRecord = createRandomGenericRecord(avroSchema)
      val newGenericRecord = AvroUtils.copyToGenericRecord(genericRecord, avroSchema, newAvroSchema)
      assert(newGenericRecord.get("custom") == null)
    }
  }

  describe("addAdditionalFieldsToSchema") {
    it ("should add additional fields to the given schema") {
      val newSchema = AvroUtils.addAdditionalFieldsToSchema(List("custom", "custom_default"), avroSchema)
      val schema: Schema = (new Schema.Parser).parse(newSchema)
      val newSchemaFieldNames = schema.getFields().asScala.map(x => x.name())
      newSchemaFieldNames.contains("custom").shouldEqual(true)
      newSchemaFieldNames.contains("custom_default").shouldEqual(true)
    }
  }

  describe("getDeserializedDataFrame") {
    it ("should deserialize the given dataframe having bytes") {
      val dataFrame = mockDataInDataFrame(10)
      val serializedDataframe = dataFrametoBytes(dataFrame, empAvroSchema)
      val deserializedDF = AvroUtils.getDeserializedDataFrame(serializedDataframe, "value", empAvroSchema)
      deserializedDF.show(1)
      assert(deserializedDF.columns.sorted.sameElements(dataFrame.columns.sorted))
    }
  }

  /*
  * Creates random avro GenericRecord for testing
  */
  def createRandomGenericRecord(avroSchema: String): GenericRecord = {
    val schema: Schema = (new Schema.Parser).parse(avroSchema)
    val it = new RandomData(schema, 1).iterator()
    val genericRecord = it.next().asInstanceOf[GenericData.Record]
    genericRecord
  }

  /*
   * Creates random avro record bytes for testing
   */
  def createRandomAvroRecordBytes(avroSchema: String): (Array[Byte], GenericRecord) = {
    val genericRecord = createRandomGenericRecord(avroSchema)
    val bytes = genericRecordToBytes(genericRecord, avroSchema)
    (bytes, genericRecord)
  }

  /*
   * Converts Generic Record to bytes given a avro schema
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

  // Mocks data for testing
  def mockAvroDataInDataFrame(numberOfRows: Int): DataFrame = {
    val dataFrame = mockDataInDataFrame(10)
    dataFrametoBytes(dataFrame, empAvroSchema)
  }

  def dataFrametoBytes(dataFrame: DataFrame, avroSchemaString: String): DataFrame = {
    import dataFrame.sparkSession.implicits._
    try {
      dataFrame.map { row =>
        val avroSchema = (new Schema.Parser).parse(avroSchemaString)
        val fields = avroSchema.getFields.asScala.map { x => x.name() }.toArray
        val cols: Map[String, Any] = row.getValuesMap(fields)
        val genericRecord: GenericRecord = new GenericData.Record(avroSchema)
        cols.foreach(x => genericRecord.put(x._1, x._2))
        // Initiate a new Java Byte Array Output Stream
        val out = new ByteArrayOutputStream()
        // Get appropriate AVRO Decoder from Factory
        try {
          val encoder = EncoderFactory.get().binaryEncoder(out, null)
          // Write the Encoded data's output (Byte Array) into the Output Stream
          // Initiate AVRO Writer from Factory
          val writer = new SpecificDatumWriter[GenericRecord](avroSchema)
          writer.write(genericRecord, encoder)
          // Flushes Data to Actual Output Stream
          encoder.flush()
          // Close the Output Stream
          out.toByteArray
        } finally {
          out.close()
        }
      }.toDF("value")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new Exception("Failed while converting DataFrame to Generic Record")
    }
  }
}
