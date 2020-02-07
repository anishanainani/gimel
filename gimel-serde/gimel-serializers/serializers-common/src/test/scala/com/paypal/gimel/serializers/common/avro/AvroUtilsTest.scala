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

import java.io.ByteArrayInputStream

import com.databricks.spark.avro.SchemaConverters.{toSqlType, SchemaType}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.trevni.avro.RandomData
import org.scalatest._

import com.paypal.gimel.serializers.common.SharedSparkSession

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

  val empAvroSchema =
    s"""{"namespace": "namespace",
          "type": "record",
          "name": "test_emp",
          "fields": [
              {\"name\": \"address\", \"type\": \"string\"},
              {\"name\": \"age\", \"type\": \"string\"},
              {\"name\": \"company\", \"type\": \"string\"},
              {\"name\": \"designation\", \"type\": \"string\"},
              {\"name\": \"id\", \"type\": \"string\"},
              {\"name\": \"name\", \"type\": \"string\"},
              {\"name\": \"salary\", \"type\": \"string\"}
         ]}""".stripMargin

  val empAvroSchema2 =
    s"""{"namespace": "namespace",
          "type": "record",
          "name": "test_emp",
          "fields": [
              {\"name\": \"address\", \"type\": \"string\"},
              {\"name\": \"age\", \"type\": \"string\"},
              {\"name\": \"company\", \"type\": \"string\"},
              {\"name\": \"designation\", \"type\": \"string\"},
              {\"name\": \"id\", \"type\": \"string\"},
              {\"name\": \"name\", \"type\": \"string\"}
         ]}""".stripMargin

  describe("dataFrametoBytes") {
    it ("should return a list of fields from avro schema") {
      val dataFrame = mockDataInDataFrame(10)
      dataFrame.show
      val serializedDataFrame = AvroUtils.dataFrametoBytes(dataFrame, empAvroSchema)
      val deserializedDf = getDeserializedDF(serializedDataFrame, empAvroSchema)
      deserializedDf.show
      assert(deserializedDf.except(dataFrame).count() == 0)
    }
  }

  describe("isDFFieldsEqualAvroFields") {
    it ("should return true if fields in dataframe are equal to fields in avro schema") {
      val dataFrame = mockDataInDataFrame(10)
      AvroUtils.isDFFieldsEqualAvroFields(dataFrame, empAvroSchema).shouldBe(true)
    }

    it ("should return false if fields in dataframe are not equal to fields in avro schema") {
      val dataFrame = mockDataInDataFrame(10)
      AvroUtils.isDFFieldsEqualAvroFields(dataFrame, empAvroSchema2).shouldBe(false)
    }
  }

  describe("genericRecordToBytes") {
    val genericRecord = createRandomGenericRecord(empAvroSchema)
    val bytes = AvroUtils.genericRecordToBytes(genericRecord, empAvroSchema)
    val deserializedRecord = bytesToGenericRecordWithSchemaRecon(bytes, empAvroSchema, empAvroSchema)
    deserializedRecord.shouldBe(genericRecord)
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

  /**
    * DeSerialize an Avro Generic Record
    *
    * @param serializedBytes A Serialized Byte Array (serialization should have been done through Avro Serialization)
    * @param writerSchema    Avro Schema String used by writer
    * @param readerSchema    Avro Schema String used by Reader
    * @return An Avro Generic Record
    */
  def bytesToGenericRecordWithSchemaRecon(serializedBytes: Array[Byte], writerSchema: String, readerSchema: String): GenericRecord = {

    try {

      // Build Avro Schema From String - for writer and reader schema
      val writerAvroSchema: Schema = (new Schema.Parser).parse(writerSchema)
      val readerAvroSchema: Schema = (new Schema.Parser).parse(readerSchema)
      // Initiate AVRO Reader from Factory
      val reader = new org.apache.avro.generic.GenericDatumReader[GenericRecord](writerAvroSchema, readerAvroSchema)
      // Initiate a new Java Byte Array Input Stream
      val in = new ByteArrayInputStream(serializedBytes)
      // Get appropriate AVRO Decoder from Factory
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      // Get AVRO generic record
      val genericRecordRead = reader.read(null, decoder)
      genericRecordRead

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  // Deserializes the given avro serialized dataframe
  def getDeserializedDF(dataFrame: DataFrame, avroSchemaString: String): DataFrame = {
    try {
      dataFrame.map { eachRow =>
        val recordToDeserialize: Array[Byte] = eachRow.getAs("value").asInstanceOf[Array[Byte]]
        // Build Avro Schema From String - for writer and reader schema
        val writerAvroSchema: Schema = (new Schema.Parser).parse(avroSchemaString)
        val readerAvroSchema: Schema = (new Schema.Parser).parse(avroSchemaString)
        // Initiate AVRO Reader from Factory
        val reader = new org.apache.avro.generic.GenericDatumReader[GenericRecord](writerAvroSchema, readerAvroSchema)
        // Initiate a new Java Byte Array Input Stream
        val in = new ByteArrayInputStream(recordToDeserialize)
        // Get appropriate AVRO Decoder from Factory
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        // Get AVRO generic record
        val deserializedGenericRecord = reader.read(null, decoder)
        // val deserializedGenericRecord: GenericRecord = bytesToGenericRecordWithSchemaRecon(recordToDeserialize, avroSchemaString, avroSchemaString)
        val avroSchemaObj: Schema = (new Schema.Parser).parse(avroSchemaString)
        val converter = AvroToSQLSchemaConverter.createConverterToSQL(avroSchemaObj)
        converter(deserializedGenericRecord).asInstanceOf[Row]
      } {
        val avroSchema: Schema = (new Schema.Parser).parse(avroSchemaString)
        val schemaType: SchemaType = toSqlType(avroSchema)
        val encoder = RowEncoder(schemaType.dataType.asInstanceOf[StructType])
        encoder
      }.toDF
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }
}
