package com.atlchain.bcgis.data;

import com.atlchain.sdk.ATLChain;

import java.io.File;
import java.util.logging.Logger;

/**
 * 区块链操作类，用于和区块链进行交互
 */
public class BlockChainClient {
    Logger logger = Logger.getLogger(BlockChainClient.class.toString());
    private ATLChain atlChain;

    BlockChainClient(File networkConfigFile) {
        this.atlChain = new ATLChain(networkConfigFile);
    }


    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    // 读取链上数据，通道名、链码名称、方法名有默认值
    public byte[][] getRecordBytes(String recordKey) {
        return getRecordBytes(recordKey, "atlchaincc", "GetByteArray");
    }

    public byte[][] getRecordBytes(String recordKey, String chaincodeName) {
        return getRecordBytes(recordKey, chaincodeName, "GetByteArray");
    }

    public byte[][] getRecordBytes(String recordKey, String chaincodeName, String functionName) {
        byte[] byteKey = recordKey.getBytes();
        byte[][] result = atlChain.queryByte(
                chaincodeName,
                functionName,
                new byte[][]{byteKey}
        );
        return result;
    }

    public String getRecord(String recordKey) {
        return getRecord(recordKey, "bcgiscc", "GetRecordByKey");
    }

    public String getRecord(String recordKey, String chaincodeName) {
        return getRecord(recordKey, chaincodeName, "GetRecordByKey");
    }

    public String getRecord(String recordKey, String chaincodeName, String functionName) {
        String key = recordKey;
        String result = atlChain.query(
                chaincodeName,
                functionName,
                new String[]{key}
        );
        return result;
    }
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    // 根据范围读取数据，范围按字典顺序排序
    public byte[][] getRecordByRange(String recordKey, String chaincodeName) {
        String startKey = recordKey + "-00000";
        String endKey = recordKey + "-99999";

        byte[][] result = atlChain.queryByte(
                chaincodeName,
                "GetRecordByKeyRange",
                new byte[][]{startKey.getBytes(), endKey.getBytes()}
        );
        return result;
    }

    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    // 向链上写数据，通道名、链码名称、方法名有默认值，默认写入字符串“record”
    public String putRecord(String recordKey, byte[] record) {
        return putRecord(recordKey, record, "bincc", "PutRecordBytes");
    }

    public String putRecord(String recordKey, byte[] record, String chaincodeName) {
        return putRecord(recordKey, record, chaincodeName, "PutRecordBytes");
    }

    public String putRecord(String recordKey, byte[] record, String chaincodeName, String functionName) {
        byte[] byteKey = recordKey.getBytes();
        String result = atlChain.invokeByte(
                chaincodeName,
                functionName,
                new byte[][]{byteKey, record}
        );
        return result;
    }

    public String putRecord(String recordKey, String record) {
        return putRecord(recordKey, record, "bincc", "PutRecordBytes");
    }

    public String putRecord(String recordKey, String record, String chaincodeName) {
        return putRecord(recordKey, record, chaincodeName, "PutRecordBytes");
    }

    public String putRecord(String recordKey, String record, String chaincodeName, String functionName) {
        String result = atlChain.invoke(
                chaincodeName,
                functionName,
                new String[]{recordKey, record}
        );
        return result;
    }
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
}
