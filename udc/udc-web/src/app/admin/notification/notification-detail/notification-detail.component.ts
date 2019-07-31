/*
 * Copyright 2019 PayPal Inc.
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

import { Component, Input, OnInit } from '@angular/core';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-notification-detail',
  templateUrl: './notification-detail.component.html',
  styleUrls: ['./notification-detail.component.scss'],
})

export class NotificationDetailDialogComponent implements OnInit {
  @Input() notification: string;
  @Input() project: string;
  public detailsLoading = false;
  public statusData = {};
  public columnList = [];
  public objectAttributesList = [];
  public isSelfDiscovered;

  constructor(private catalogService: CatalogService, private sessionService: SessionService) {
  }

  ngOnInit() {
    this.getObjectDetails();
  }

  getObjectDetails() {
    this.detailsLoading = true;
    this.catalogService.getNotificationDetails(this.notification)
      .subscribe(data => {
        this.statusData = data;
        this.detailsLoading = false;
      }, error => {
        this.statusData = {};
        this.detailsLoading = false;
      });
  }

}