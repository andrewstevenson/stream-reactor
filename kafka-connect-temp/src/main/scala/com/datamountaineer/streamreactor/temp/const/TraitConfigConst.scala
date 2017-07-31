/*
 * Copyright 2017 Datamountaineer.
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

package com.datamountaineer.streamreactor.temp.const

object TraitConfigConst {
  val KCQL_PROP_SUFFIX = "kcql"
  val ERROR_POLICY_PROP_SUFFIX = "error.policy"
  val RETRY_INTERVAL_PROP_SUFFIX = "retry.interval"
  val MAX_RETRIES_PROP_SUFFIX = "max.retries"
  val BATCH_SIZE_PROP_SUFFIX = "batch.size"
  val DATABASE_PROP_SUFFIX = "db"
  val THREAD_POLL_PROP_SUFFIX = "threadpool.size"
  val ALLOW_PARALLEL_WRITE_PROP_SUFFIX = "parallel.write"
  val CONSISTENCY_LEVEL_PROP_SUFFIX = "consistency.level"
  val USERNAME_SUFFIX = "username"
  val PASSWORD_SUFFIX = "password"
  val AUTH_MECH_SUFFIX = "auth.mechanism"
  val TRUSTSTORE_PASS_SUFFIX = "truststore.pass"
  val TRUSTSTORE_PATH_SUFFIX = "truststore.path"
  val KEYSTORE_PASS_SUFFIX = "keystore.pass"
  val KEYSTORE_PATH_SUFFIX = "keystore.path"
  val CERTIFICATE_SUFFIX = "certs"
  val CERTIFICATE_KEY_CHAIN_SUFFIX = "cert.chain.key"
  val PROGRESS_ENABLED = "connect.progress.enabled"
  val CONNECT_ERROR_POLICY = "connect.error.policy"
  val URI_SUFFIX = "uri"
  val BIND_HOST_SUFFIX = "bind.host"
  val BIND_PORT_SUFFIX = "bind.port"
}
