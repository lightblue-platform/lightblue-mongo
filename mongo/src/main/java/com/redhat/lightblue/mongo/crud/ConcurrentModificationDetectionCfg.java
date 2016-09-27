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
package com.redhat.lightblue.mongo.crud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.redhat.lightblue.config.ControllerConfiguration;

/**
 * This class parses the concurrent modification detection settings
 * from the controller configuration and keeps them
 *
 * <pre>
 *   options: {
 *     concurrentModification: {
 *      detection: true,
 *      failureRetryCount:3,
 *      reevaluateQueryForRetry: true
 *    }
 * </pre>
 */
public class ConcurrentModificationDetectionCfg {
    private boolean detect=true;
    private int failureRetryCount=3;
    private boolean reevaluateQueryForRetry=true;

    public ConcurrentModificationDetectionCfg(ControllerConfiguration controllerCfg) {
        if(controllerCfg!=null) {
            ObjectNode options=controllerCfg.getOptions();
            if(options!=null) {
                options=(ObjectNode)options.get("concurrentModification");
                if(options!=null) {
                    JsonNode value=options.get("detection");
                    if(value!=null)
                        detect=options.asBoolean();
                    value=options.get("failureRetryCount");
                    if(value!=null)
                        failureRetryCount=value.asInt();
                    value=options.get("reevaluateQueryForRetry");
                    if(value!=null)
                        reevaluateQueryForRetry=value.asBoolean();
                }
            }
        }
    }

    /**
     * If true, concurrent modification detection is enabled. If
     * false, concurrent modifications will overwrite each other
     * without any notification
     */
    public boolean isDetect() {
        return detect;
    }

    public void setDetect(boolean b) {
        detect=b;
    }

    /**
     * Number of times to retry failed updates
     */
    public int getFailureRetryCount() {
        return failureRetryCount;
    }

    public void setFailureRetryCount(int n) {
        failureRetryCount=n;
    }

    /**
     * If true, during the retry phase of failed concurrent
     * modifications, updater validates that the failed document still
     * matches the query before retrying the update.
     */
    public boolean isReevaluateQueryForRetry() {
        return reevaluateQueryForRetry;
    }

    public void setReevaluateQueryForRetry(boolean b) {
        reevaluateQueryForRetry=b;
    }
}
