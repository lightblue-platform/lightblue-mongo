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
package com.redhat.lightblue.interceptor;

import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.mediator.OperationContext;

public class InterceptPoint {

    public static final class MediatorInterceptPoint extends InterceptPoint {
        private MediatorInterceptPoint(int code) {
            super(MediatorInterceptor.class,code);
        }

        public void call(Interceptor t,OperationContext ctx) {
            ((MediatorInterceptor)t).run(ctx);
        }
    }

    public static final class CRUDControllerInterceptPoint extends InterceptPoint {
        private CRUDControllerInterceptPoint(int code) {
            super(CRUDControllerInterceptor.class,code);
        }

        public void call(Interceptor t,CRUDOperationContext ctx) {
            ((CRUDControllerInterceptor)t).run(ctx);
        }
    }

    public static final class CRUDDocInterceptPoint extends InterceptPoint {
        private CRUDDocInterceptPoint(int code) {
            super(CRUDDocInterceptor.class,code);
        }

        public void call(Interceptor t,CRUDOperationContext ctx,DocCtx doc) {
            ((CRUDDocInterceptor)t).run(ctx,doc);
        }
    }

    public static final MediatorInterceptPoint PRE_MEDIATOR_INSERT=new MediatorInterceptPoint(0);
    public static final MediatorInterceptPoint POST_MEDIATOR_INSERT=new MediatorInterceptPoint(1);
    public static final CRUDControllerInterceptPoint PRE_CRUD_INSERT=new CRUDControllerInterceptPoint(2);
    public static final CRUDControllerInterceptPoint POST_CRUD_INSERT=new CRUDControllerInterceptPoint(3);

    public static final MediatorInterceptPoint PRE_MEDIATOR_SAVE=new MediatorInterceptPoint(4);
    public static final MediatorInterceptPoint POST_MEDIATOR_SAVE=new MediatorInterceptPoint(5);
    public static final CRUDControllerInterceptPoint PRE_CRUD_SAVE=new CRUDControllerInterceptPoint(6);
    public static final CRUDControllerInterceptPoint POST_CRUD_SAVE=new CRUDControllerInterceptPoint(7);
    public static final CRUDDocInterceptPoint PRE_CRUD_SAVE_UPDATE_DOC=new CRUDDocInterceptPoint(8);
    public static final CRUDDocInterceptPoint POST_CRUD_SAVE_UPDATE_DOC=new CRUDDocInterceptPoint(9);
    public static final CRUDDocInterceptPoint PRE_CRUD_SAVE_INSERT_DOC=new CRUDDocInterceptPoint(10);
    public static final CRUDDocInterceptPoint POST_CRUD_SAVE_INSERT_DOC=new CRUDDocInterceptPoint(11);

    public static final MediatorInterceptPoint PRE_MEDIATOR_UPDATE=new MediatorInterceptPoint(12);
    public static final MediatorInterceptPoint POST_MEDIATOR_UPDATE=new MediatorInterceptPoint(13);
    public static final CRUDControllerInterceptPoint PRE_CRUD_UPDATE=new CRUDControllerInterceptPoint(14);
    public static final CRUDControllerInterceptPoint POST_CRUD_UPDATE=new CRUDControllerInterceptPoint(15);
    public static final CRUDControllerInterceptPoint PRE_CRUD_UPDATE_RESULTSET=new CRUDControllerInterceptPoint(16);
    public static final CRUDControllerInterceptPoint POST_CRUD_UPDATE_RESULTSET=new CRUDControllerInterceptPoint(17);
    public static final CRUDDocInterceptPoint PRE_CRUD_UPDATE_DOC=new CRUDDocInterceptPoint(18);
    public static final CRUDDocInterceptPoint POST_CRUD_UPDATE_DOC=new CRUDDocInterceptPoint(19);


    public static final MediatorInterceptPoint PRE_MEDIATOR_DELETE=new MediatorInterceptPoint(20);
    public static final MediatorInterceptPoint POST_MEDIATOR_DELETE=new MediatorInterceptPoint(21);
    public static final CRUDControllerInterceptPoint PRE_CRUD_DELETE=new CRUDControllerInterceptPoint(22);
    public static final CRUDControllerInterceptPoint POST_CRUD_DELETE=new CRUDControllerInterceptPoint(23);
    public static final CRUDDocInterceptPoint PRE_CRUD_DELETE_DOC=new CRUDDocInterceptPoint(24);
    public static final CRUDDocInterceptPoint POST_CRUD_DELETE_DOC=new CRUDDocInterceptPoint(25);

    public static final MediatorInterceptPoint PRE_MEDIATOR_FIND=new MediatorInterceptPoint(26);
    public static final MediatorInterceptPoint POST_MEDIATOR_FIND=new MediatorInterceptPoint(27);
    public static final CRUDControllerInterceptPoint PRE_CRUD_FIND=new CRUDControllerInterceptPoint(28);
    public static final CRUDControllerInterceptPoint POST_CRUD_FIND=new CRUDControllerInterceptPoint(29);
    public static final CRUDDocInterceptPoint POST_CRUD_FIND_DOC=new CRUDDocInterceptPoint(30);

    private final Class interceptorClass;
    private final int code;

    private InterceptPoint(Class interceptorClass,int code) {
        this.interceptorClass=interceptorClass;
        this.code=code;
    }

    public Class getInterceptorClass() {
        return interceptorClass;
    }

    @Override
    public int hashCode() {
        return code;
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof InterceptPoint)
            return ((InterceptPoint)o).code==code;
        return false;
    }
}