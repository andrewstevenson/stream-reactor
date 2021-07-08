/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package io.lenses.streamreactor.connect.azure.storage.sinks.writers

import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageConfig
import org.apache.kafka.connect.sink.SinkRecord

trait Writer {
  def write(name: String, records: Seq[SinkRecord], batchSize: Int = AzureStorageConfig.TABLE_DEFAULT_BATCH_SIZE): Unit
}
