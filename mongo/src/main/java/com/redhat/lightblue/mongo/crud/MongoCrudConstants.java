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

public final class MongoCrudConstants {

    public static final String ERR_INVALID_OBJECT = "mongo-crud:InvalidObject";
    public static final String ERR_DUPLICATE = "mongo-crud:Duplicate";
    public static final String ERR_INSERTION_ERROR = "mongo-crud:InsertionError";
    public static final String ERR_SAVE_ERROR = "mongo-crud:SaveError";
    public static final String ERR_UPDATE_ERROR = "mongo-crud:UpdateError";
    public static final String ERR_NO_ACCESS = "mongo-crud:NoAccess";
    public static final String ERR_CONNECTION_ERROR = "mongo-crud:ConnectionError";

    public static final String ERR_EMPTY_DOCUMENTS = "mongo-crud:EmptyDocuments";
    public static final String ERR_EMPTY_VALUE_LIST = "mongo-crud:EmptyValueList";

    public static final String ERR_NULL_QUERY = "mongo-crud:NullQuery";
    public static final String ERR_NULL_PROJECTION = "mongo-crud:NullProjection";

    public static final String ERR_SAVE_CLOBBERS_HIDDEN_FIELDS = "mongo-crud:SaveClobblersHiddenFields";
    public static final String ERR_TRANSLATION_ERROR = "mongo-crud:TranslationError";

    public static final String ERR_ENTITY_INDEX_NOT_CREATED = "mongo-crud:EntityIndexNotCreated";
    public static final String ERR_INVALID_INDEX_FIELD = "mongo-crud:InvalidIndexField";
    public static final String ERR_DUPLICATE_INDEX = "mongo-crud:DuplicateIndex";

    public static final String ERR_INVALID_LOCKING_DOMAIN = "mongo-crud:InvalidLockingDomain";
    public static final String ERR_CONFIGURATION_ERROR = "mongo=crud:ConfigurationError";

    public static final String ERR_NO_SEQUENCE_NAME = "mongo-crud:NoSequenceName";

    public static final String ERR_TOO_MANY_RESULTS = "mongo-crud:TooManyResults";
    public static final String ERR_RESERVED_FIELD = "mongo-crud:ReservedFieldInMetadata";

    private MongoCrudConstants() {

    }
}
