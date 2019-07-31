/*
 * Copyright 2017 PayPal Inc.
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

package com.paypal.gimel.jdbc.utilities

import java.sql.{BatchUpdateException, Connection, DriverManager, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.collection.immutable.Map

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructField

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, GimelConstants}
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.exception._
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities.getJDBCSystem
import com.paypal.gimel.logger.Logger


/**
  * JDBC implementation internal to PCatalog
  * This implementation will be used to read from any JDBC data sources e.g. MYSQL, TERADATA
  */
object JDBCUtilities {
  def apply(sparkSession: SparkSession): JDBCUtilities = new JDBCUtilities(sparkSession)
}

class JDBCUtilities(sparkSession: SparkSession) extends Serializable {

  // val logger = Logger(this.getClass)
  val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)

  /**
    * This method reads the data from JDBC data source into dataFrame
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                dataSetprops is the way to set various additional parameters for read and write operations in DataSet class
    *                Example UseCase :
    *                We can specify the additional parameters for databases using JDBC.
    *                e.g. For "Teradata", exporting data, specify "teradata.read.type" parameter as "FASTEXPORT" (optional)
    * @return DataFrame
    */
  def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {

    val logger = Logger(this.getClass.getName)
    // logger.setLogLevel("CONSOLE")
    // sparkSession.conf.set("gimel.logging.level", "CONSOLE")

    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)

    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val jdbcURL = jdbcOptions("url")
    val dbtable = jdbcOptions("dbtable")

    // get connection
    var conn: Connection = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

    // get partitionColumn
    // val userPartitionColumn: Option[Any] = dataSetProps.get("partitionColumn")
    val userPartitionColumn: Option[Any] = Some("10")
    val partitionColumn = userPartitionColumn match {

      case None =>
        // get numeric only primary index of the table if not specified
        val primaryIndices: Seq[String] = JdbcAuxiliaryUtilities.getPrimaryKeys(jdbcURL, dbtable, conn, true)
        val defaultPartitionColumn: String = {
          if (!primaryIndices.isEmpty) {
            primaryIndices(0)
          }
          else {
            JdbcConstants.noPartitionColumn
          }
        }
        defaultPartitionColumn

      case _ =>
        userPartitionColumn.get.toString
    }

    // get lowerBound & upperBound
    val (lowerBoundValue: Double, upperBoundValue: Double) = if (!partitionColumn.equals(JdbcConstants.noPartitionColumn)) {
      logger.info(s"Partition column is set to ${partitionColumn}")
      JdbcAuxiliaryUtilities.getMinMax(partitionColumn, dbtable, conn)
    }
    else {
      (0.0, 20.0)
    }

    val lowerBound = dataSetProps.getOrElse("lowerBound", lowerBoundValue.floor.toLong).toString.toLong
    val upperBound = dataSetProps.getOrElse("upperBound", upperBoundValue.ceil.toLong).toString.toLong

    // set number of partitions
    val userSpecifiedPartitions = dataSetProps.get("numPartitions")
    val numPartitions: Int = JdbcAuxiliaryUtilities.getNumPartitions(jdbcURL, userSpecifiedPartitions, JdbcConstants.readOperation)

    partitionColumn match {
      case JdbcConstants.noPartitionColumn =>
        logger.info(s"Number of partitions are set to 1 with NO partition column.")
        1
      case _ =>
        logger.info(s"Read Operation is set with numPartitions=${numPartitions} for partitionColumn=${partitionColumn} with lowerBound=${lowerBound} and upperBound=${upperBound}")
    }

    val fetchSize = dataSetProps.getOrElse("fetchSize", JdbcConstants.defaultReadFetchSize).toString.toInt


    val jdbcSystem = getJDBCSystem(jdbcURL)
    try {
      jdbcSystem match {
        case JdbcConstants.TERADATA => {
          val dbConnection = new DbConnection(jdbcConnectionUtility)
          val selectStmt = s"SELECT * FROM ${dbtable} WHERE ?<=$partitionColumn AND $partitionColumn<=?"

          // set jdbcPushDownFlag to false if using through dataset.read
          logger.info(s"Setting jdbcPushDownFlag to FALSE in TaskContext")
          sparkSession.sparkContext.setLocalProperty(JdbcConfigs.jdbcPushDownEnabled, "false")

          val jdbcRDD: ExtendedJdbcRDD[Array[Object]] = new ExtendedJdbcRDD(sparkSession.sparkContext, dbConnection, selectStmt, lowerBound, upperBound, numPartitions, fetchSize)

          conn = if (conn.isClosed || conn == null) {
            jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
          }
          else {
            conn
          }

          // getting table schema to build final dataframe
          val tableSchema = JdbcReadUtility.resolveTable(jdbcURL, dbtable, conn)
          val rowRDD: RDD[Row] = jdbcRDD.map(v => Row(v: _*))
          sparkSession.createDataFrame(rowRDD, tableSchema)
        }
        case _ =>
          // default spark JDBC read
          JdbcAuxiliaryUtilities.sparkJdbcRead(sparkSession, jdbcURL, dbtable, partitionColumn, lowerBound, upperBound, numPartitions, fetchSize, jdbcConnectionUtility.getConnectionProperties())
      }
    }
    catch {
      case exec: SQLException => {
        var ex: SQLException = exec
        while (ex != null) {
          ex.printStackTrace()
          ex = ex.getNextException
        }
        throw exec
      }
      case e: Throwable =>
        throw e
    }
    finally {
      // re-setting all configs for read
      JDBCCommons.resetReadConfigs(sparkSession)
    }
  }


  /**
    * This method writes the dataframe into given JDBC datasource
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param dataFrame    The DataFrame to write into Target table
    * @param dataSetProps Any more options to provide
    * @return DataFrame
    */
  def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {

    val logger = Logger(this.getClass.getName)
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    var jdbc_url = jdbcOptions("url")

    val batchSize: Int = dataSetProps.getOrElse("batchSize", s"${JdbcConstants.defaultWriteBatchSize}").toString.toInt
    val dbtable = jdbcOptions("dbtable")
    val teradataType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString
    val insertStrategy: String = dataSetProps.getOrElse(JdbcConfigs.jdbcInsertStrategy, s"${JdbcConstants.defaultInsertStrategy}").toString

    // get number of user partitions
    val userSpecifiedPartitions = dataSetProps.get("numPartitions")

    // Get specific URL properties
    // get real user of JDBC
    val realUser: String = JDBCCommons.getJdbcUser(dataSetProps, sparkSession)

    // get password strategy for JDBC
    val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.jdbcDefaultPasswordStrategy).toString

    val partialArgHolder = if (insertStrategy.equalsIgnoreCase("update") || insertStrategy.equalsIgnoreCase("upsert")) {
      // get Set columns for update API
      val setColumns: List[String] = {
        val userSpecifiedSetColumns = dataSetProps.getOrElse(JdbcConfigs.jdbcUpdateSetColumns, null)
        if (userSpecifiedSetColumns == null) {
          dataFrame.columns.toList
        }
        else {
          userSpecifiedSetColumns.toString.split(",").toList
        }
      }

      // get WHERE columns for update API
      val whereColumns: List[String] = {
        val userSpecifiedWhereColumns = dataSetProps.getOrElse(JdbcConfigs.jdbcUpdateWhereColumns, null)
        if (userSpecifiedWhereColumns == null) {
          val primaryKeys = JdbcAuxiliaryUtilities.getPrimaryKeys(jdbc_url, dbtable, jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(), false)
          primaryKeys
        }
        else {
          userSpecifiedWhereColumns.toString.split(",").toList
        }
      }
      logger.info(s"Setting SET columns: ${setColumns}")
      logger.info(s"Setting WHERE columns: ${whereColumns}")
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, realUser, jdbcPasswordStrategy, dbtable, _: Int, dataFrame.schema.length, setColumns, whereColumns)
    }
    else {
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, realUser, jdbcPasswordStrategy, dbtable, _: Int, dataFrame.schema.length)
    }


    if (insertStrategy.equalsIgnoreCase("FullLoad")) {
      val dbconn: Connection = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

      // truncate table
      JdbcAuxiliaryUtilities.truncateTable(jdbc_url, dbtable, dbconn)

      dbconn.close()
    }

    val (insetBatchSize, insertMethod) = (insertStrategy, teradataType) match {
      case ("update", _) =>
        (0, updateTable _)
      case ("upsert", _) =>
        (0, upsertTable _)
      case (_, _) =>
        (batchSize, insertParallelBatch _)
    }

    val sourceDataFrame = JdbcAuxiliaryUtilities.getPartitionedDataFrame(jdbc_url, dataFrame, userSpecifiedPartitions)

    val jdbcHolder = partialArgHolder(insetBatchSize)

    try {
      insertMethod(sourceDataFrame, jdbcConnectionUtility, jdbcHolder)
      dataFrame
    }
    catch {
      case exec: Throwable =>
        throw exec
    }
    finally {
      // re-setting all configs for write
      JDBCCommons.resetWriteConfigs(sparkSession)
    }
  }

  /**
    * This method returns the index of the column in array of columns
    *
    * @param dataFramecolumns Array of columns in dataframe
    * @param column           column for which index has to be found
    * @return Int
    */
  def getColumnIndex(dataFramecolumns: Array[String], column: String): Int = {
    dataFramecolumns.indexWhere(column.equalsIgnoreCase)
  }


  /**
    * It cooks the given preparedStatement with the given row.
    * It uses the columns whose index are given in `columnIndices` and sets parameters at indeices
    * given in `paramIndices`.
    *
    * @param st
    * @param row
    * @param columnIndices
    * @param paramIndices
    */
  private def cookStatementWithRow(st: PreparedStatement, row: Row,
                                   columnIndices: Seq[Int],
                                   paramIndices: Seq[Int]): PreparedStatement = {
    require(columnIndices.size == paramIndices.size,
      s"Different number of column and param indices were passed to $st.")
    columnIndices.zip(paramIndices).foreach { case (columnIndex, paramIndex) =>
      val columnValue = row.get(columnIndex)
      val targetSqlType = SparkToJavaConverter.getSQLType(row.schema(columnIndex).dataType)
      try {
        st.setObject(paramIndex, columnValue, targetSqlType)
      }
      catch {
        case e: Throwable =>
          val msg =
            s"""
               |Error setting column value=${columnValue} at column index=${columnIndex}
               |with input dataFrame column dataType=${row.schema(columnIndex).dataType} to target dataType=${targetSqlType}
            """.stripMargin
          throw new SetColumnObjectException(msg)

      }
    }
    st
  }


  //  /**
  //    * This method returns the hive schema for a given hive table as StructType
  //    * if the table is catalogued, the schema will be coming from the meta store (pcatalog/HIVE/USER)
  //    * if the table is NOT catalogued, we will query the respective storage to get the schema
  //    *
  //    * @param dataSetProps Dataset Properties
  //    * @return StructType
  //    *         The hive schema for the given hive table
  //    */
  //  def getHiveSchema(dataSetProps: Map[String, Any]): StructType = {
  //    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  //    val hiveTable: String = actualProps.props.get(GimelConstants.RESOLVED_HIVE_TABLE).toString
  //    val catalogProvider = dataSetProps.get(CatalogProviderConfigs.CATALOG_PROVIDER).get.toString
  //    val schema = catalogProvider match {
  //      case com.paypal.gimel.common.conf.CatalogProviderConstants.PCATALOG_PROVIDER => {
  //
  //        if (actualProps.props.get(CatalogProviderConstants.DYNAMIC_DATASET).get == "true") {
  //          val columns = getTableSchema(dataSetProps)
  //          val schema: Array[StructField] = columns.map(x => {
  //            org.apache.spark.sql.types.StructField(x.columName, SparkToJavaConverter.getSparkTypeFromTeradataDataType(x.columnType.toUpperCase()), true)
  //          }).toArray
  //          new StructType(schema)
  //        }
  //        else {
  //          val schema: Array[StructField] = actualProps.fields.map(x => {
  //            org.apache.spark.sql.types.StructField(x.fieldName, SparkToJavaConverter.getSparkTypeFromTeradataDataType(x.fieldType.toUpperCase()), true)
  //          })
  //          new StructType(schema)
  //        }
  //      }
  //      case com.paypal.gimel.common.conf.CatalogProviderConstants.HIVE_PROVIDER => {
  //        val table: DataFrame = sparkSession.sql(s"SELECT * FROM $hiveTable")
  //        table.schema
  //      }
  //    }
  //    schema
  //  }

  /**
    * This method inserts into given table in given mode
    *
    * @param dataFrame
    * @param jdbcConnectionUtility
    * @param jdbcHolder
    */
  private def insertParallelBatch(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {

    val jdbcSystem = JdbcAuxiliaryUtilities.getJDBCSystem(jdbcHolder.jdbcURL)
    var ddl = ""
    jdbcSystem match {
      case JdbcConstants.TERADATA => {
        // create a JDBC connection to get DDL of target table
        val driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

        // get DDL of target table
        ddl = JdbcAuxiliaryUtilities.getDDL(jdbcHolder.jdbcURL, jdbcHolder.dbTable, driverCon)
        driverCon.close()
      }
      case _ => {
        // do nothing
      }
    }


    // For each partition create a temp table to insert
    dataFrame.foreachPartition { batch =>

      // create logger inside the executor
      val logger = Logger(this.getClass.getName)

      // get the partition ID
      val partitionID = TaskContext.getPartitionId()

      // creating a new connection for every partition
      // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection for every partition.
      // Singleton connection connection needs to be correctly verified within multiple cores.
      var dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
      JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)

      val partitionTableName = jdbcSystem match {
        case JdbcConstants.TERADATA => {
          val partitionTableName = JdbcAuxiliaryUtilities.getPartitionTableName(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc, partitionID)

          // first drop the temp table, if exists
          JdbcAuxiliaryUtilities.dropTable(partitionTableName, dbc)

          try {

            // create JDBC temp table
            val tempTableDDL = ddl.replace(jdbcHolder.dbTable, partitionTableName)

            logger.info(s"Creating temptable: ${partitionTableName} with DDL = ${tempTableDDL}")

            // create a temp partition table
            JdbcAuxiliaryUtilities.executeQuerySatement(tempTableDDL, dbc)
          }
          catch {
            case ex: Throwable =>
              val msg = s"Failure creating temp partition table ${partitionTableName}"
              logger.info(msg + "\n" + s"${ex.toString}")
              throw new JDBCPartitionException(msg)
          }
          partitionTableName
        }
        case _ =>
          jdbcHolder.dbTable
      }

      // close the connection, if batch is empty, so that we don't hold connection
      if (batch.isEmpty) {
        dbc.close()
      }

      if (batch.nonEmpty) {
        logger.info(s"Inserting into $partitionTableName")

        val maxBatchSize = math.max(1, jdbcHolder.batchSize)
        try {

          // check if connection is closed or null
          if (dbc.isClosed || dbc == null) {
            dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
            JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)
          }

          val st = dbc.prepareStatement(JdbcAuxiliaryUtilities.getInsertStatement(partitionTableName, jdbcHolder.cols))

          logger.info(s"Inserting to temptable ${partitionTableName} of Partition: ${partitionID}")

          // set AutoCommit to FALSE
          dbc.setAutoCommit(false)

          var count = 0
          var rowCount = 0

          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              st.addBatch()
              rowCount = rowCount + 1
            }
            try {
              st.executeBatch()
              count = count + 1
            }
            catch {
              case exec: Throwable => {
                exec match {
                  case batchExc: BatchUpdateException =>
                    logger.info(s"Exception in inserting data into ${partitionTableName} of Partition: ${partitionID}")
                    var ex: SQLException = batchExc
                    while (ex != null) {
                      ex.printStackTrace()
                      ex = ex.getNextException
                    }
                    throw batchExc
                  case _ =>
                    logger.info(s"Exception in inserting data into ${partitionTableName} of Partition: ${partitionID}")
                    throw exec
                }
              }
            }
          }
          logger.info(s"Data Insert successful into ${partitionTableName} of Partition: ${partitionID}")

          // commit
          dbc.commit()

          // close the connection
          dbc.close()

        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }
      }
    }

    jdbcSystem match {
      case JdbcConstants.TERADATA => {
        // create a JDBC connection to get DDL of target table
        val driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
        JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, driverCon)

        // Now union all the temp partition tables and insert into target table
        try {
          // now insert all partitions into target table
          JdbcAuxiliaryUtilities.insertPartitionsIntoTargetTable(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)

          // now drop all the temp tables created by executors
          JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)
          driverCon.close()
        }
        catch {
          case e: Throwable => e.printStackTrace()

            // get or create a JDBC connection to get DDL of target table
            val driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
            JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, driverCon)

            // now drop all the temp tables created by executors
            JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)
            driverCon.close()
            throw e
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!driverCon.isClosed && driverCon != null) {
            driverCon.close()
          }
        }

      }
      case _ => {
        // do nothing
      }
    }
  }

  /**
    * This method is used to UPDATE into the table
    *
    * @param dataFrame  dataFrame to be loaded into table
    * @param jdbcHolder jdbc arguments required for writing into table
    */
  private def updateTable(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {

    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {

        // creating a new connection for every partition
        // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection for every partition.
        // Singleton connection connection needs to be correctly verified within multiple cores.
        val dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
        JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)

        try {
          val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable, jdbcHolder.setColumns, jdbcHolder.whereColumns)
          val st: PreparedStatement = dbc.prepareStatement(updateStatement)
          val maxBatchSize = math.max(1, jdbcHolder.batchSize)
          val setColumnIndices = jdbcHolder.setColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          val whereColumnIndices = jdbcHolder.whereColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, setColumnIndices, 1 to setColumnIndices.length)
              // set the condition variables
              val whereColumnIndicesUpdated = (1 to whereColumnIndices.length).map(_ + setColumnIndices.length)
              cookStatementWithRow(st, row, whereColumnIndices, whereColumnIndicesUpdated)
              st.addBatch()
            }
            try {
              st.executeBatch()
            }
            catch {
              case exec: Throwable => {
                exec match {
                  case batchExc: BatchUpdateException =>
                    var ex: SQLException = batchExc
                    while (ex != null) {
                      ex.printStackTrace()
                      ex = ex.getNextException
                    }
                    throw batchExc
                  case _ =>
                    throw exec
                }
              }
            }
          }
        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }

      }
    }
  }

  /**
    * This method is used to UPSERT into the table
    *
    * @param dataFrame
    * @param jDBCConnectionUtility
    * @param jdbcHolder
    */
  private def upsertTable(dataFrame: DataFrame, jDBCConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        val dbc = jDBCConnectionUtility.getJdbcConnectionAndSetQueryBand()
        JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)
        try {
          val numCols: Int = jdbcHolder.cols
          val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable, jdbcHolder.setColumns, jdbcHolder.whereColumns)
          val st: PreparedStatement = dbc.prepareStatement(updateStatement)
          val setColumnIndices = jdbcHolder.setColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          val whereColumnIndices = jdbcHolder.whereColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          batch.foreach { row =>
            cookStatementWithRow(st, row, setColumnIndices, 1 to setColumnIndices.length)
            // set the condition variables
            val whereColumnIndicesUpdated = (1 to whereColumnIndices.length).map(_ + setColumnIndices.length)
            cookStatementWithRow(st, row, whereColumnIndices, whereColumnIndicesUpdated)
            // update table
            val updateResult: Int = st.executeUpdate()
            // if no update, then insert into table
            if (updateResult == 0) {
              val st = dbc.prepareStatement(JdbcAuxiliaryUtilities.getInsertStatement(jdbcHolder.dbTable, numCols))
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              // insert into table
              try {
                st.executeUpdate()
              }
              catch {
                case exec: Throwable => {
                  exec match {
                    case batchExc: BatchUpdateException =>
                      var ex: SQLException = batchExc
                      while (ex != null) {
                        ex.printStackTrace()
                        ex = ex.getNextException
                      }
                      throw batchExc
                    case _ =>
                      throw exec
                  }
                }
              }
            }
          }
        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }
      }
    }
  }


  /** This method creates the table in teradata database
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def create(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val logger = Logger(this.getClass.getName)
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val catalogProvider = dataSetProps.get(CatalogProviderConfigs.CATALOG_PROVIDER).get.toString
    val sql = dataSetProps.get(GimelConstants.TABLE_SQL).get.toString
    logger.info("sql statement " + sql)
    val createTableStatement = catalogProvider match {
      case com.paypal.gimel.common.conf.CatalogProviderConstants.PCATALOG_PROVIDER | com.paypal.gimel.common.conf.CatalogProviderConstants.UDC_PROVIDER => {
        dataSetProps.get(GimelConstants.CREATE_STATEMENT_IS_PROVIDED).get match {
          case "true" =>
            // Since create statement is provided, we do not need to prepare the statement instead we need
            // to pass the sql as is to the teradata engine.
            sql
          case _ =>
            // As the create statement is not provided we need to infer schema from the dataframe.
            // In GimelQueryProcesser we already infer schema and pass the columns with their data types
            // we need construct the create statement
            JDBCUtilityFunctions.prepareCreateStatement(sql, jdbcOptions("dbtable"), dataSetProps)
        }
      }
      case GimelConstants.USER => {
        val colList: Array[String] = actualProps.fields.map(x => (x.fieldName + " " + (x.fieldType) + ","))
        val conCatColumns = colList.mkString("")
        s"""CREATE TABLE ${jdbcOptions("dbtable")} (${conCatColumns.dropRight(1)} ) """
      }
    }
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(createTableStatement)
    columnStatement.execute()
  }

  /**
    * prepareCreateStatement - From the column details passed in datasetproperties from GimelQueryProcesser,
    * create statement is constructed. If user passes primary index , set or multi set table, those will be added in the create statement
    *
    * @param dbtable      - Table Name
    * @param dataSetProps - Data set properties
    * @return - the created prepared statement for creating the table
    */
  def prepareCreateStatement(sql: String, dbtable: String, dataSetProps: Map[String, Any]): String = {
    // Here we remove the SELECT portion and have only the CREATE portion of the DDL supplied so that we can use that to create the table
    val sqlParts = sql.split(" ")
    val lenPartKeys = sqlParts.length
    val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
    val createOnly: String = sqlParts.slice(0, index - 1).mkString(" ")

    // Here we remove the PCATALOG prefix => we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
    val createParts = createOnly.split(" ")
    val pcatSQL = createParts.map(element => {
      if (element.toLowerCase().contains(GimelConstants.PCATALOG_STRING) || element.toLowerCase().contains(GimelConstants.UDC_STRING) ) {
        // we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
        element.split('.').tail.mkString(".").split('.').tail.mkString(".").split('.').tail.mkString(".")
      }
      else {
        element
      }
    }
    ).mkString(" ")

    val sparkSchema = dataSetProps(GimelConstants.TABLE_FILEDS).asInstanceOf[Array[StructField]]
    // From the dataframe schema, translate them into Teradata data types
    val gimelSchema: Array[com.paypal.gimel.common.catalog.Field] = sparkSchema.map(x => {
      com.paypal.gimel.common.catalog.Field(x.name, SparkToJavaConverter.getTeradataDataType(x.dataType), x.nullable)
    })
    val colList: Array[String] = gimelSchema.map(x => (x.fieldName + " " + (x.fieldType) + ","))
    val conCatColumns = colList.mkString("").dropRight(1)
    val colQulifier = s"""(${conCatColumns})"""

    // Here we inject back the columns with data types back in the SQL statemnt
    val newSqlParts = pcatSQL.split(" ")
    val PCATindex = newSqlParts.indexWhere(_.toUpperCase().contains("TABLE"))
    val catPrefix = newSqlParts.slice(0, PCATindex + 2).mkString(" ")
    val catSuffix = newSqlParts.slice(PCATindex + 2, newSqlParts.length).mkString(" ")
    val fullStatement = s"""${catPrefix} ${colQulifier} ${catSuffix}"""
    fullStatement
  }

  /** This method drops the table from teradata database
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def drop(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val dropTableStatement = s"""DROP TABLE ${jdbcOptions("dbtable")}"""
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(dropTableStatement)
    columnStatement.execute()
  }

  /** This method purges data in the table
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def truncate(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val dropTableStatement = s"""DELETE FROM ${jdbcOptions("dbtable")}"""
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(dropTableStatement)
    columnStatement.execute()
  }

}

/**
  * case class to define column
  *
  * @param columName
  * @param columnType
  */
case class Column(columName: String, columnType: String)
