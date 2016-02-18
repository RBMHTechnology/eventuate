/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.example.dbreplica.repository

import java.sql.Driver
import javax.sql.DataSource

import com.typesafe.config.Config
import org.hsqldb.jdbcDriver

import org.springframework.jdbc.datasource.SimpleDriverDataSource
import org.springframework.jdbc.datasource.embedded._

class StorageBackend(config: Config) {
  val endpointId: String =
    config.getString("eventuate.endpoint.id")

  val dataSource: DataSource =
    new EmbeddedDatabaseBuilder().setDataSourceFactory(dataSourceFactory(s"jdbc:hsqldb:file:target/DB/EP-$endpointId")).addScript("dbreplica/create.sql").build()

  def dataSourceFactory(url: String): DataSourceFactory = new DataSourceFactory {
    private val dataSource: SimpleDriverDataSource = new SimpleDriverDataSource

    dataSource.setDriverClass(classOf[jdbcDriver])
    dataSource.setUrl(url)
    dataSource.setUsername("sa")
    dataSource.setPassword("")

    def getDataSource: DataSource =
      dataSource

    def getConnectionProperties: ConnectionProperties = new ConnectionProperties() {
      def setDriverClass(driverClass: Class[_ <: Driver]) = ()
      def setUsername(username: String) = ()
      def setPassword(password: String) = ()
      def setUrl(url: String) = ()
    }
  }
}
