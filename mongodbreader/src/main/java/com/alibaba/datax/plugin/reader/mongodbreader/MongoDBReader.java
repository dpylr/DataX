package com.alibaba.datax.plugin.reader.mongodbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;
        private MongoClient mongoClient;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.mongoClient = MongoUtil.initMongoConnection(this.originalConfig);
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;
        private String query = null;

        private JSONArray mongodbColumnMeta = null;

        private Long skipCount = null;
        private Long limitSize = null;
        private int taskCnt = 1;

        @Override
        public void startRead(RecordSender recordSender) {

            if (limitSize == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);
            BsonDocument sort = new BsonDocument();
            sort.append(KeyConstant.MONGO_PRIMIARY_ID_META, new BsonInt32(1));

            MongoCursor<Document> dbCursor;
            if (this.taskCnt > 1) {
                if (!Strings.isNullOrEmpty(query)) {
                    dbCursor = col.find(BsonDocument.parse(query)).sort(sort).skip(skipCount.intValue()).limit(this.limitSize.intValue()).iterator();
                } else {
                    dbCursor = col.find().sort(sort).skip(skipCount.intValue()).limit(this.limitSize.intValue()).iterator();
                }
            } else {
                if (!Strings.isNullOrEmpty(query)) {
                    dbCursor = col.find(BsonDocument.parse(query)).skip(skipCount.intValue()).limit(this.limitSize.intValue()).iterator();
                } else {
                    dbCursor = col.find().skip(skipCount.intValue()).limit(this.limitSize.intValue()).iterator();
                }
            }
            while (dbCursor.hasNext()) {
                Document document = dbCursor.next();
                Record record = recordSender.createRecord();
                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject) columnItera.next();
                    String columnName = column.getString(KeyConstant.COLUMN_NAME);
                    String columnType = column.getString(KeyConstant.COLUMN_TYPE).trim().toLowerCase();
                    Object tempCol;
                    if (KeyConstant.ALL_COLUMN.equals(columnName)) {
                        tempCol = document.toJson();
                    } else {
                        tempCol = document.get(columnName);
                    }

                    if (tempCol == null) {
                        record.addColumn(new StringColumn());
                    } else if (columnType.equals(KeyConstant.STRING_TYPE) || Strings.isNullOrEmpty(columnType)) {
                        record.addColumn(new StringColumn(tempCol.toString()));
                    } else if (tempCol instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    } else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else {
                        if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if (Strings.isNullOrEmpty(splitter)) {
                                splitter = KeyConstant.DEFAULT_COLUMN_SPLITTER;
                            }
                            ArrayList array = (ArrayList) tempCol;
                            String tempArrayStr = Joiner.on(splitter).join(array);
                            record.addColumn(new StringColumn(tempArrayStr));
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.mongoClient = MongoUtil.initMongoConnection(this.readerSliceConfig);

            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.limitSize = readerSliceConfig.getLong(KeyConstant.LIMIT_SIZE);
            this.skipCount = readerSliceConfig.getLong(KeyConstant.SKIP_COUNT);
            this.taskCnt = readerSliceConfig.getInt(KeyConstant.TASK_CNT);

        }

        @Override
        public void destroy() {

        }
    }
}
