package com.alibaba.datax.plugin.reader.mongodbreader;

public class KeyConstant {
    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";
    /**
     * 字符串类型
     */
    public static final String STRING_TYPE = "string";
    /**
     * mongodb 的 MONGO_CLIENT_URI 例如 mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1
     * http://mongodb.github.io/mongo-java-driver/3.2/driver/reference/connecting/connection-settings/
     * http://api.mongodb.com/java/3.2/?com/mongodb/MongoClientURI.html
     */
    public static final String MONGO_CLIENT_URI = "URI";
    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_ADDRESS = "address";
    /**
     * mongodb 的用户名
     */
    public static final String MONGO_USER_NAME = "userName";
    /**
     * mongodb 密码
     */
    public static final String MONGO_USER_PASSWORD = "userPassword";
    /**
     * mongodb 数据库名
     */
    public static final String MONGO_DB_NAME = "dbName";
    /**
     * mongodb 认证数据库
     */
    public static final String MONGO_AUTH_DB_NAME = "authDbName";
    /**
     * mongodb 默认认证数据库admin
     */
    public static final String MONGO_ADMIN_DB = "admin";
    /**
     * mongodb 是否启用Server Discovery
     */
    public static final String SERVER_DISCOERY = "serverDiscovery";
    /**
     * mongodb ReadPreference
     包含以下5种
     primary
     primaryPreferred
     secondary
     secondaryPreferred
     nearest
     */
    public static final String READ_PERFERENCE = "readPreference";
    public static final String READ_PERFERENCE_PRIMARY = "primary";
    public static final String READ_PERFERENCE_PRIMARYPREFERRED = "primaryPreferred";
    public static final String READ_PERFERENCE_SECONDARY = "secondary";
    public static final String READ_PERFERENCE_SECONDARYPREFERRED = "secondaryPreferred";
    public static final String READ_PERFERENCE_NEAREST = "nearest";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "collectionName";
    /**
     * mongodb 查询条件
     * JSON中格式:
     * "query": "{\"cTimeStamp\":{\"$gte\":ISODate(\"2016-10-31T16:00:00.000Z\"),
     *                             \"$lt\" : ISODate(\"2016-10-31T16:30:00.000Z\")}}",
     */
    public static final String MONGO_QUERY = "query";
    /**
     * mongodb 的列
     */
    public static final String MONGO_COLUMN = "column";
    /**
     * mongodb 的全字段输出
     */
    public static final String ALL_COLUMN = "ALL_COLUMN";

    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 列分隔符
     */
    public static final String DEFAULT_COLUMN_SPLITTER = "|";
    /**
     * 跳过的列数
     */
    public static final String SKIP_COUNT = "skipCount";
    /**
     * 批量获取的记录数
     */
    public static final String LIMIT_SIZE = "limitSize";
    /**
     * MongoDB的idmeta
     */
    public static final String MONGO_PRIMIARY_ID_META = "_id";

    /**
     * 同步任务并行数
     */
    public static final String TASK_CNT = "taskCnt";

    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equals(type);
    }
}
