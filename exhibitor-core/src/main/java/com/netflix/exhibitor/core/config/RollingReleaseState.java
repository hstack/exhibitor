/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.config;

import com.netflix.exhibitor.core.state.InstanceState;
import com.netflix.exhibitor.core.state.ServerList;

class RollingReleaseState
{
    private final InstanceState     currentInstanceState;
    private final ConfigCollection  config;

    RollingReleaseState(InstanceState currentInstanceState, ConfigCollection config)
    {
        this.currentInstanceState = currentInstanceState;
        this.config = config;
    }

    String getCurrentRollingHostname()
    {
        return config.getRollingConfigState().getRollingHostNames().get(config.getRollingConfigState().getRollingHostNamesIndex());
    }

  /**
   * Whether rolling config server list has the same elements
   * @return
   */
  boolean serverListHasSynced()
    {
        String      targetServersSpec = config.getRollingConfig().getString(StringConfigs.SERVERS_SPEC);
        ServerList  targetServerList = new ServerList(targetServersSpec);
        return targetServerList.equals(currentInstanceState.getServerList());
    }
}
