package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoUtil {

    public static MongoClient initMongoClient(Configuration conf) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if (addressList == null || addressList.size() <= 0) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        try {
            if (conf.getBool(KeyConstant.SERVER_DISCOERY, true)) {
                return new MongoClient(parseServerAddress(addressList));
            } else {
                return new MongoClient(parseServerAddress(addressList).get(0));
            }
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
        }
    }

    public static MongoClient initCredentialMongoClient(Configuration conf, String userName, String password, String database) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if (!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        try {
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            if (conf.getBool(KeyConstant.SERVER_DISCOERY, true)) {
                return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));
            } else {
                return new MongoClient(parseServerAddress(addressList).get(0), Arrays.asList(credential));
            }

        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
        }
    }

    public static MongoClient initMongoConnection(Configuration conf) {
        MongoClient mongoClient;
        String mongoClientURI = null;
        String userName = null;
        String password = null;

        String database = null;
        String authDatabase = null;
        String readPreference = null;

        mongoClientURI = conf.getString(KeyConstant.MONGO_CLIENT_URI);
        if (!Strings.isNullOrEmpty(mongoClientURI)) {
            mongoClient = new MongoClient(new MongoClientURI(mongoClientURI));
        } else {
            userName = conf.getString(KeyConstant.MONGO_USER_NAME);
            password = conf.getString(KeyConstant.MONGO_USER_PASSWORD);
            database = conf.getString(KeyConstant.MONGO_DB_NAME);
            authDatabase = conf.getString(KeyConstant.MONGO_AUTH_DB_NAME);
            if (Strings.isNullOrEmpty(authDatabase)) {
                authDatabase = KeyConstant.MONGO_ADMIN_DB;
            }

            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(conf, userName, password, authDatabase);
            } else {
                mongoClient = MongoUtil.initMongoClient(conf);
            }
            readPreference = conf.getString(KeyConstant.READ_PERFERENCE);
            if (!Strings.isNullOrEmpty(readPreference)) {
                mongoClient.setReadPreference(MongoUtil.getReadPreference(readPreference));
            }
        }

        return mongoClient;
    }

    /**
     * 判断地址类型是否符合要求
     *
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        for (Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if (!((String) address).matches(regex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 转换为mongo地址协议
     *
     * @param rawAddressList
     * @return
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList) throws UnknownHostException {
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for (Object address : rawAddressList) {
            String[] tempAddress = ((String) address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0], Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }

    /**
     * 获取对应的readPreference
     *
     * @param readPreference
     * @return
     */
    public static ReadPreference getReadPreference(String readPreference) {
        if (readPreference.equals(KeyConstant.READ_PERFERENCE_PRIMARYPREFERRED)) {
            return ReadPreference.primaryPreferred();
        } else if (readPreference.equals(KeyConstant.READ_PERFERENCE_SECONDARY)) {
            return ReadPreference.secondary();
        } else if (readPreference.equals(KeyConstant.READ_PERFERENCE_SECONDARYPREFERRED)) {
            return ReadPreference.secondaryPreferred();
        } else if (readPreference.equals(KeyConstant.READ_PERFERENCE_NEAREST)) {
            return ReadPreference.nearest();
        } else {
            return ReadPreference.primary();
        }
    }

}
