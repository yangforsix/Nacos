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

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.utils.InstanceUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service storage.
 *
 * @author xiweng.yy
 */
@Component
public class ServiceStorage {
    
    private final ClientServiceIndexesManager serviceIndexesManager;
    
    private final ClientManager clientManager;
    
    private final SwitchDomain switchDomain;
    
    private final NamingMetadataManager metadataManager;
    
    private final ConcurrentMap<Service, ServiceInfo> serviceDataIndexes;
    
    private final ConcurrentMap<Service, Set<String>> serviceClusterIndex;
    
    public ServiceStorage(ClientServiceIndexesManager serviceIndexesManager, ClientManagerDelegate clientManager,
            SwitchDomain switchDomain, NamingMetadataManager metadataManager) {
        this.serviceIndexesManager = serviceIndexesManager;
        this.clientManager = clientManager;
        this.switchDomain = switchDomain;
        this.metadataManager = metadataManager;
        this.serviceDataIndexes = new ConcurrentHashMap<>();
        this.serviceClusterIndex = new ConcurrentHashMap<>();
    }
    
    public Set<String> getClusters(Service service) {
        return serviceClusterIndex.getOrDefault(service, new HashSet<>());
    }
    
    public ServiceInfo getData(Service service) {
        // 如果缓存中存在了该服务信息，则直接返回
        // 否则就调用getPushData方法
        return serviceDataIndexes.containsKey(service) ? serviceDataIndexes.get(service) : getPushData(service);
    }
    
    public ServiceInfo getPushData(Service service) {
        // 根据临时服务对象生成一个空的ServiceInfo
        ServiceInfo result = emptyServiceInfo(service);
        // 如果该服务没有进行注册就直接返回空的服务信息对象ServiceInfo
        if (!ServiceManager.getInstance().containSingleton(service)) {
            return result;
        }
        // 如果注册了，就从缓存中获取到这个Service对象
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        // 根据这个服务对象获取到它的所有实例信息
        result.setHosts(getAllInstancesFromIndex(singleton));
        // 将这个服务的信息放入【服务数据】缓存中 <Service, ServiceInfo>
        serviceDataIndexes.put(singleton, result);
        return result;
    }
    
    public void removeData(Service service) {
        serviceDataIndexes.remove(service);
        serviceClusterIndex.remove(service);
    }
    
    private ServiceInfo emptyServiceInfo(Service service) {
        ServiceInfo result = new ServiceInfo();
        result.setName(service.getName());
        result.setGroupName(service.getGroup());
        result.setLastRefTime(System.currentTimeMillis());
        result.setCacheMillis(switchDomain.getDefaultPushCacheMillis());
        return result;
    }
    
    private List<Instance> getAllInstancesFromIndex(Service service) {
        Set<Instance> result = new HashSet<>();
        Set<String> clusters = new HashSet<>();
        // 从之前注册的缓存中获取到这个服务的所有注册过的实例连接信息
        for (String each : serviceIndexesManager.getAllClientsRegisteredService(service)) {
            // 获取对应的实例发布信息
            Optional<InstancePublishInfo> instancePublishInfo = getInstanceInfo(each, service);
            if (instancePublishInfo.isPresent()) {
                // 如果存在实例发布信息
                // 就获取发布信息
                InstancePublishInfo publishInfo = instancePublishInfo.get();
                // 将发布信息中的实例信息添加到返回的实例集合中
                //If it is a BatchInstancePublishInfo type, it will be processed manually and added to the instance list
                if (publishInfo instanceof BatchInstancePublishInfo) {
                    BatchInstancePublishInfo batchInstancePublishInfo = (BatchInstancePublishInfo) publishInfo;
                    List<Instance> batchInstance = parseBatchInstance(service, batchInstancePublishInfo, clusters);
                    result.addAll(batchInstance);
                } else {
                    Instance instance = parseInstance(service, instancePublishInfo.get());
                    result.add(instance);
                    clusters.add(instance.getClusterName());
                }
            }
        }
        // 放入到【服务 - 集群】缓存中
        // cache clusters of this service
        serviceClusterIndex.put(service, clusters);
        // 结果返回
        return new LinkedList<>(result);
    }
    
    /**
     * Parse batch instance.
     * @param service service
     * @param batchInstancePublishInfo batchInstancePublishInfo
     * @return batch instance list
     */
    private List<Instance> parseBatchInstance(Service service, BatchInstancePublishInfo batchInstancePublishInfo, Set<String> clusters) {
        List<Instance> resultInstanceList = new ArrayList<>();
        List<InstancePublishInfo> instancePublishInfos = batchInstancePublishInfo.getInstancePublishInfos();
        for (InstancePublishInfo instancePublishInfo : instancePublishInfos) {
            Instance instance = parseInstance(service, instancePublishInfo);
            resultInstanceList.add(instance);
            clusters.add(instance.getClusterName());
        }
        return resultInstanceList;
    }

    // 根据连接信息id找到对应的实例发布信息
    private Optional<InstancePublishInfo> getInstanceInfo(String clientId, Service service) {
        Client client = clientManager.getClient(clientId);
        if (null == client) {
            return Optional.empty();
        }
        return Optional.ofNullable(client.getInstancePublishInfo(service));
    }
    
    private Instance parseInstance(Service service, InstancePublishInfo instanceInfo) {
        Instance result = InstanceUtil.parseToApiInstance(service, instanceInfo);
        Optional<InstanceMetadata> metadata = metadataManager
                .getInstanceMetadata(service, instanceInfo.getMetadataId());
        metadata.ifPresent(instanceMetadata -> InstanceUtil.updateInstanceMetadata(result, instanceMetadata));
        return result;
    }
}
