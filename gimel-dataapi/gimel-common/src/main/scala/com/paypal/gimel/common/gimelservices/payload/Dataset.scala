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


/*
 * A dataset is an entity in Metastore which would ultimately translate to a hive external table.
 * Http End point which gives you this payload is http://localhost:8080/dataSet/dataSet
 * Sample Request -> {"createdUser": "drampally", "storageDataSetName": "wuser_holding_bdpe","storageSystemId":56,
 * "storageDataSetDescription": "wuser_holding on bdpe","storageContainerName":"default",
 * "clusters":[4,5,6,7,8],"dataSetAttributeValues": [{"storageDsAttributeValue": "wuser_holding",
 * "createdUser": "drampally","storageDsAttributeKeyId": 8}],"userId": 2,"isAutoRegistered":"Y"}
 */

package com.paypal.gimel.common.gimelservices.payload
case class Dataset(
                        createdUser: String = "default"
                      , storageDataSetName: String = "default"
                      , storageSystemId: Int = 1
                      , storageDataSetDescription: String = ""
                      , storageContainerName: String = ""
                      , objectName: String = ""
                      , clusters: Seq[Int]
                      , userId: Int
                      , isAutoRegistered: String = "Y"
                  )
