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

package com.paypal.gimel.hbase.utilities

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.security.AuthHandler
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.logger.Logger

/**
  * HBASE implementations internal to PCatalog
  */
object HBaseUtilities {

  def apply(sparkSession: SparkSession): HBaseUtilities = new HBaseUtilities(sparkSession)

}

class HBaseUtilities(sparkSession: SparkSession) {
  val logger = Logger()
  val thisUser: String = sys.env(GimelConstants.USER)

  val hiveDataSet = new com.paypal.gimel.hive.DataSet(sparkSession)
  val hbaseLookUp = HBaseLookUp(sparkSession)
  val hbasePut = HBasePut(sparkSession)
  val hbaseSparkConnector = HBaseSparkConnector(sparkSession)

  /**
    * This function performs scan/bulkGet on hbase table
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                data.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  def read(dataset: String, dataSetProps: Map[String, Any] = Map.empty): DataFrame = {
    try {
      /**
        * If condition for API switch. Default(false) goes to Native API.
        */
      if (dataSetProps.getOrElse(HbaseConfigs.hbaseOperation, "scan").asInstanceOf[String].equals("get")) {
        hbaseLookUp.get(dataset, dataSetProps)
      } else {
        if (dataSetProps.getOrElse("useHive", false).asInstanceOf[Boolean]) {
          logger.info("useHive is true, using Hive API")
          hiveDataSet.read(dataset, dataSetProps)
        } else {
          logger.info("useHive is false or not set, using Native API")
          hbaseSparkConnector.read(dataset, dataSetProps)
        }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw HBaseDataSetException("Error reading from HBase table")
    }
  }


  /**
    * This function performs bulk write into hbase table
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  Example Usecase : we want only 1 executor for hbase (specifically)
    *                  val props = Map("coalesceFactor" -> 1)
    *                  Dataset(sc).write(clientDataFrame, props)
    *                  Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */

  def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    try {
      /**
        * If condition for API switch. Default(false) goes to Native API.
        */

      // if (dataSetProps.get.getOrElse("useHive", false).asInstanceOf[Boolean]) {
      // Overriding write API to use only SHC.
      // https://issues.apache.org/jira/browse/SPARK-6628
      // logger.warning("'useHive' switch is overridden due to https://issues.apache.org/jira/browse/SPARK-6628. We are forcing write to use SHC API.")

      val castedDataFrame = castAllColsToString(dataFrame)
      if (dataSetProps.getOrElse(HbaseConfigs.hbaseOperation, "scan").asInstanceOf[String].equals("put")) {
        hbasePut.put(dataset, castedDataFrame, dataSetProps)
      } else {
        if (false) {
          logger.info("useHive is true, using Hive API")
          hiveDataSet.write(dataset, castedDataFrame, dataSetProps)
        } else {
          logger.info("useHive is false or not set, using Native API")
          hbaseSparkConnector.write(dataset, castedDataFrame, dataSetProps)
        }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw HBaseDataSetException("Error writing data to HBase table")
    }
  }

  case class HBaseDataSetException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

  /**
    *
    * @param dataDrame DataFrame to cast all columns to string format.
    * @return Dataframe with all string data.
    */
  private def castAllColsToString(dataDrame: DataFrame): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info("Casting All Columns as String")
    val k = dataDrame.schema.fieldNames.foldRight(dataDrame) {
      (column: String, df: DataFrame) => df.withColumn(column, df(column).cast(StringType))
    }
    logger.info("Coalescing All Columns with Null Values to Empty String")
    val returningDF = k.schema.fieldNames.foldRight(k) {
      (fieldName: String, df: DataFrame) => df.withColumn(fieldName, coalesce(df(fieldName), lit("")))
    }
    logger.info("Done with Column Coalese operation")
    returningDF
  }

  /**
    * Authenticate Read/Write with HBASE Policies
    *
    * @param dataset
    * @param operation
    * @param dataSetProps
    */
  def authenticateThroughRangerPolicies(dataset: String, operation: String, dataSetProps: Map[String, Any]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val tableProperties = datasetProps.props
    val hbaseTable = dataSetProps.getOrElse(HbaseConfigs.hbaseTableKey, tableProperties.getOrElse(HbaseConfigs.hbaseTableKey, "")).asInstanceOf[String]
    val hbaseNameSpace = dataSetProps.getOrElse(GimelConstants.HBASE_NAMESPACE, tableProperties.getOrElse(GimelConstants.HBASE_NAMESPACE, "default")).asInstanceOf[String]
    val hbaseTableName = hbaseTable.split(":")(1)
    val hBaseNameSpaceAndTable = hbaseNameSpace + ":" + hbaseTableName
    val clusterName = com.paypal.gimel.common.utilities.DataSetUtils.getYarnClusterName()
    logger.info("hBaseNameSpaceAndTable and clusterName" + hBaseNameSpaceAndTable + clusterName)
    val currentUser = datasetProps.props.getOrElse(GimelConstants.GTS_USER_CONFIG, sparkSession.sparkContext.sparkUser)
    if (AuthHandler.isAuthRequired(sparkSession)) {
      AuthHandler.authenticateHbasePolicy(currentUser, operation, hBaseNameSpaceAndTable, dataset, clusterName)
    }
  }
}

