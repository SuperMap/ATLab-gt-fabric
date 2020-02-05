package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.supermap.blockchain.sdk.SmChain;

import java.io.File;
import java.util.logging.Logger;

/**
 * 区块链操作类，用于和区块链进行交互
 */
public class BlockChainClient {
    Logger logger = Logger.getLogger(BlockChainClient.class.toString());
    private SmChain smChain;

    BlockChainClient(File networkConfigFile) {
        smChain = SmChain.getSmChain("txchannel", networkConfigFile);
    }

    // 读取链上数据，通道名、链码名称、方法名有默认值
    public byte[][] getRecordBytes(String recordKey) {
        return getRecordBytes(recordKey, "atlchaincc", "GetByteArray");
    }

    public byte[][] getRecordBytes(String recordKey, String chaincodeName) {
        return getRecordBytes(recordKey, chaincodeName, "GetByteArray");
    }

    public byte[][] getRecordBytes(String recordKey, String chaincodeName, String functionName) {
        byte[] byteKey = recordKey.getBytes();
//        byte[][] result = atlChain.queryByte(
//                chaincodeName,
//                functionName,
//                new byte[][]{byteKey}
//        );
        byte[][] result = smChain.getSmTransaction().queryByte(
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
        String result = smChain.getSmTransaction().query(
                chaincodeName,
                functionName,
                new String[]{key}
        );
        return result;
    }

    /**
     * 构造 selector 语句进行属性查询（测试用）
     * @param chaincodeName
     * @param functionName
     * @param selector
     * @return
     */
    public String getRecordBySeletor(String chaincodeName, String functionName, String selector) {
        String result = smChain.getSmTransaction().query(
                chaincodeName,
                functionName,
                new String[]{selector}
        );
        return result;
    }

    /**
     * 构造 selector 语句进行分页属性查询
     * @param chaincodeName
     * @param functionName
     * @param selector
     * @param page
     * @param bookMark
     * @return
     */
    public String getRecordBySeletorByPage(String chaincodeName, String functionName, String selector, String page, String bookMark) {
        String result = smChain.getSmTransaction().query(
                chaincodeName,
                functionName,
                new String[]{selector, page, bookMark}
        );
        return result;
    }

    /**
     * 根据范围读取数据，范围按字典顺序排序
     * @param recordKey
     * @param chaincodeName
     * @return
     */
    public byte[][] getRecordByRange(String recordKey, String chaincodeName) {
        String startKey = recordKey + "-00000";
        String endKey = recordKey + "-99999";
        byte[][] result = smChain.getSmTransaction().queryByte(
                chaincodeName,
                "GetRecordByKeyRange",
                new byte[][]{startKey.getBytes(), endKey.getBytes()}
        );
        return result;
    }

    // 2019.12.19根据提示的范围进行范围查询geometry（startkey包含------endkey不包含）

    /**
     * 根据自定义分页 JSONArray 和方法 GetRecordByKeyRangeByte 查询得到 byte[][] 形式的空间几何数据然后组合成结果返回
     * @param recordKey
     * @param chaincodeName
     * @param jsonArray
     * @return
     */
    public byte[][] getRecordByRangeByte(String recordKey, String chaincodeName, JSONArray jsonArray) {
        int tempRang = jsonArray.get(jsonArray.size() - 1).toString().length() + 2;
        byte[][] byteMerger = null;
        String startKey;
        String endKey;
        for(int i = 0; i < jsonArray.size() -1; i ++){
            startKey = recordKey + "-" + String.format("%0" + tempRang + "d", jsonArray.get(i));
            endKey = recordKey + "-" + String.format("%0" + tempRang + "d", Integer.parseInt(jsonArray.getString(i + 1)));
            byte[][] result = smChain.getSmTransaction().queryByte(
                    chaincodeName,
                    "GetRecordByKeyRangeByte",
                    new byte[][]{startKey.getBytes(), endKey.getBytes()}
            );
            byteMerger = byteMerger(byteMerger, result, i);
            result = null;
        }
        return byteMerger;
    }

    /**
     * 根据自定义分页 JSONArray 和方法 GetRecordByKeyRange 查询得到 byte[][] 形式的空间几何数据的属性然后组合成结果返回
     * @param recordKey
     * @param chaincodeName
     * @param jsonArray
     * @return
     */
    public byte[][] getRecordByRange(String recordKey, String chaincodeName, JSONArray jsonArray) {

        int tempRang = jsonArray.get(jsonArray.size() - 1).toString().length() + 2;
        byte[][] byteMerger = null;
        String startKey;
        String endKey;
        for(int i = 0; i < jsonArray.size() -1; i ++){
            startKey = recordKey + "-" + String.format("%0" + tempRang + "d", jsonArray.get(i));
            endKey = recordKey + "-" + String.format("%0" + tempRang + "d", Integer.parseInt(jsonArray.getString(i + 1)));
            byte[][] result = smChain.getSmTransaction().queryByte(
                    chaincodeName,
                    "GetRecordByKeyRange",
                    new byte[][]{startKey.getBytes(), endKey.getBytes()}
            );
            byteMerger = byteMerger(byteMerger, result, i);
            result = null;
        }
        return byteMerger;
    }

    /**
     * 合并 byte[][]
     * @param bt1
     * @param bt2
     * @param count
     * @return
     */
    public static byte[][] byteMerger(byte[][] bt1, byte[][] bt2, int count){
        byte[][] byteMerger = null;
        if(count == 0){
            byteMerger = bt2;
        }else{
            byteMerger = new byte[bt1.length+bt2.length][];
            System.arraycopy(bt1, 0, byteMerger, 0, bt1.length);
            System.arraycopy(bt2, 0, byteMerger, bt1.length, bt2.length);
        }
        return byteMerger;
    }

    // 向链上写数据，通道名、链码名称、方法名有默认值，默认写入字符串“record”
    public String putRecord(String recordKey, byte[] record) {
        return putRecord(recordKey, record, "bincc", "PutRecordBytes");
    }

    public String putRecord(String recordKey, byte[] record, String chaincodeName) {
        return putRecord(recordKey, record, chaincodeName, "PutRecordBytes");
    }

    public String putRecord(String recordKey, byte[] record, String chaincodeName, String functionName) {
        byte[] byteKey = recordKey.getBytes();
        String result = smChain.getSmTransaction().invokeByte(
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
        String result = smChain.getSmTransaction().invoke(
                chaincodeName,
                functionName,
                new String[]{recordKey, record}
        );
        return result;
    }

    /**
     * 删除当前世界状态库指定键的数据
     */
    public String deleteRecord(String recordKey, String chaincodeName, String functionName) {
        String result = smChain.getSmTransaction().invoke(
                chaincodeName,
                functionName,
                new String[]{recordKey}
        );
        return result;
    }

}
