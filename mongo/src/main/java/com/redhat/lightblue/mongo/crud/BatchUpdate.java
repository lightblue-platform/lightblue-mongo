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

import java.util.Map;

import com.mongodb.DBObject;

import com.redhat.lightblue.util.Error;

public interface BatchUpdate {

    /**
     * Reset the batch update
     */
    void reset();
    
    /**
     * Adds a document to the current batch. The document should
     * contain the original docver as read from the db
     */
    void addDoc(DBObject doc);
    
    /**
     * Returns the number of queued requests
     */
    int getSize();

    /**
     * Commits the current batch, and prepares for the next
     * iteration. If any update operations fail, this call will detect
     * errors and associate them with the documents using the document
     * index.
     */
    Map<Integer,Error> commit();

}
