/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.minion.tasks.upsertcompactmerge;

import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;


@TaskExecutorFactory
public class UpsertCompactMergeTaskExecutorFactory implements PinotTaskExecutorFactory {

  private MinionConf _minionConf;

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
  }

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager, MinionConf minionConf) {
    _minionConf = minionConf;
  }

  @Override
  public String getTaskType() {
    return MinionConstants.UpsertCompactMergeTask.TASK_TYPE;
  }

  @Override
  public PinotTaskExecutor create() {
    return new UpsertCompactMergeTaskExecutor(_minionConf);
  }
}
