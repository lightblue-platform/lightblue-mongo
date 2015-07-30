/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.crud.mongo;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.mongodb.DB;

import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.extensions.synch.LockingSupport;
import com.redhat.lightblue.extensions.synch.Locking;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.common.mongo.MongoDataStore;

public class MongoLockingSupport implements LockingSupport {

    private static final Logger LOGGER=LoggerFactory.getLogger(MongoLockingSupport.class);

    private final MongoCRUDController controller;
    
    public MongoLockingSupport(MongoCRUDController controller) {
        this.controller=controller;
    }

    @Override
    public String[] getLockingDomains() {
        List<String> list=new ArrayList<>();
        LOGGER.debug("Getting configured locking domains");
        ControllerConfiguration cfg=controller.getControllerConfiguration();
        if(cfg!=null) {
            LOGGER.debug("Got controller configuration");
            ObjectNode configNode=cfg.getExtensions();
            if(configNode!=null) {
                LOGGER.debug("Extensions: {}",configNode);
                JsonNode x=configNode.get("locking");
                if(x instanceof ArrayNode) {
                    ArrayNode arr=(ArrayNode)x;
                    LOGGER.debug("Locking:{}",arr);
                    for(Iterator<JsonNode> domains=arr.elements();
                        domains.hasNext();) {
                        x=domains.next();
                        if(x instanceof ObjectNode) {
                            ObjectNode domain=(ObjectNode)x;
                            JsonNode domainName=domain.get("domain");
                            if(domainName!=null)
                                list.add(domainName.asText());
                        }
                    }
                }
            }
        }
        LOGGER.debug("Domains:{}",list);
        return list.toArray(new String[list.size()]);
    }

    @Override
    public Locking getLockingInstance(String domain) {
        ObjectNode domainNode=findDomainNode(domain);
        if(domainNode==null)
            throw Error.get(MongoCrudConstants.ERR_INVALID_LOCKING_DOMAIN,domain);
        JsonNode datasourceName=domainNode.get("datasource");
        if(datasourceName==null)
            throw Error.get(MongoCrudConstants.ERR_CONFIGURATION_ERROR,"locking."+domain+".datasource");
        MongoDataStore store=new MongoDataStore();
        store.setDatasourceName(datasourceName.asText());
        DB db=controller.getDbResolver().get(store);
        if(db==null)
            throw Error.get(MongoCrudConstants.ERR_CONFIGURATION_ERROR,"locking."+domain+".datasource");
        JsonNode collection=domainNode.get("collection");
        if(collection==null)
            throw Error.get(MongoCrudConstants.ERR_CONFIGURATION_ERROR,"locking."+domain+".collection");
        return new MongoLocking(db.getCollection(collection.asText()));
    }

    private ObjectNode findDomainNode(String domain) {
        ControllerConfiguration cfg=controller.getControllerConfiguration();
        if(cfg!=null) {
            ObjectNode configNode=cfg.getExtensions();
            if(configNode!=null) {
                JsonNode x=configNode.get("locking");
                if(x instanceof ArrayNode) {
                    ArrayNode arr=(ArrayNode)x;
                    for(Iterator<JsonNode> domains=arr.elements();
                        domains.hasNext();) {
                        x=domains.next();
                        if(x instanceof ObjectNode) {
                            ObjectNode d=(ObjectNode)x;
                            JsonNode domainName=d.get("domain");
                            if(domainName!=null&&domainName.asText().equals(domain))
                                return d;
                        }
                    }
                }
            }
        }
        return null;
    }
}
