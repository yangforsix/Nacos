/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.push.v2.task;

import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;

import java.util.HashSet;
import java.util.Set;

/**
 * Nacos naming push delay task.
 *
 * @author xiweng.yy
 */
public class PushDelayTask extends AbstractDelayTask {
    
    private final Service service;
    
    private boolean pushToAll;
    
    private Set<String> targetClients;
    
    public PushDelayTask(Service service, long delay) {
        this.service = service;
        pushToAll = true;
        targetClients = null;
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }
    
    public PushDelayTask(Service service, long delay, String targetClient) {
        this.service = service;
        this.pushToAll = false;
        this.targetClients = new HashSet<>(1);
        this.targetClients.add(targetClient);
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }
    
    @Override
    public void merge(AbstractDelayTask task) {
        // 这里的传入task是已经存在的任务
        if (!(task instanceof PushDelayTask)) {
            // 如果原来的任务不是PushDelayTask就不做处理
            return;
        }
        PushDelayTask oldTask = (PushDelayTask) task;
        // 是否是要推送到所有实例，根据上一个任务的要求设置和本任务兼容的值
        if (isPushToAll() || oldTask.isPushToAll()) {
            pushToAll = true;
            targetClients = null;
        } else {
            // 如果两个任务都不需要推送到所有实例，那么就只需要把客户端信息添加到任务需要推送的客户端信息列表中即可
            targetClients.addAll(oldTask.getTargetClients());
        }
        // 修改最后的处理事件
        setLastProcessTime(Math.min(getLastProcessTime(), task.getLastProcessTime()));
        Loggers.PUSH.info("[PUSH] Task merge for {}", service);
    }
    
    public Service getService() {
        return service;
    }
    
    public boolean isPushToAll() {
        return pushToAll;
    }
    
    public Set<String> getTargetClients() {
        return targetClients;
    }
}
