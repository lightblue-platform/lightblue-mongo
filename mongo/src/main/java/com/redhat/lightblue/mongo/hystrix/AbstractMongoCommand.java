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
package com.redhat.lightblue.mongo.hystrix;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.mongodb.DBCollection;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoException;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

/**
 * The groupkey for all mongodb commands are "mongodb"
 *
 * @author nmalik
 */
public abstract class AbstractMongoCommand<T> extends HystrixCommand<T> {

    /**
     * The groupkey for all mongodb commands are "mongodb"
     */
    public static final String GROUPKEY = "mongodb";

    private final DBCollection collection;

    /**
     *
     * @param commandKey REQUIRED
     * @param threadPoolKey OPTIONAL defaults to groupKey value
     * @param collection REQUIRED
     */
    public AbstractMongoCommand(String commandKey, DBCollection collection) {
        super(HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(GROUPKEY)).
                andCommandKey(HystrixCommandKey.Factory.asKey(GROUPKEY + ":" + commandKey)).
                andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(GROUPKEY)));
        this.collection = collection;
    }

    protected DBCollection getDBCollection() {
        return collection;
    }

    @Override
    protected T run() {
        try {
            return runMongoCommand();
        } catch (MongoSocketException mse) {
            // must rethrow because MongoSocketException is instance of RuntimeException
            throw mse;
        } catch(MongoException me) {
            throw me;
        } catch (RuntimeException x) {
            StringWriter sw=new StringWriter();
            PrintWriter w=new PrintWriter(sw);
            w.println(x.toString());
            x.printStackTrace(w);
            throw new HystrixBadRequestException("in " + getClass().getName()+":"+sw.toString(),x);
        }
    }

    protected abstract T runMongoCommand();

    public T executeAndUnwrap() {
        try {
            return execute();
        } catch (HystrixRuntimeException x) {
            Throwable t=x.getCause();
            if(t instanceof RuntimeException)
                throw (RuntimeException)t;
            else
                throw new RuntimeException(t);
        }
    }
}
