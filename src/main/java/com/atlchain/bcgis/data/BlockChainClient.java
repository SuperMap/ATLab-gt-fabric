package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import com.atlchain.sdk.ATLChain;

import java.io.File;
import java.util.List;
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

    // TODO 后期属性的增加在这里设置 String[] 自动获取里面的全部参数
    public String getRecord(List<String> list, String chaincodeName, String functionName) {
        String result = atlChain.query(
                chaincodeName,
                functionName,
                new String[]{list.get(0), list.get(1)}
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

//    // TODO 2019.12.29根据提示的范围进行范围查询（startkey包含------endkey不包含）
//    public byte[][] getRecordByRange(String recordKey, String chaincodeName, JSONArray jsonArray) {
//
//        int tempRang = jsonArray.get(jsonArray.size() - 1).toString().length() + 2;
//        byte[][] byteMerger = null;
//        String startKey;
//        String endKey;
//        String tmpFile = "E:\\SuperMapData\\BCGISDataStoreTmpFile";
//
//        for(int i = 0; i < jsonArray.size() -1; i ++){
//            // TODO 先判断本地有没有，有就直接用，没有就读取
//            File file = new File(tmpFile + File.separator + recordKey + String.valueOf(i));
//            byte[][] result =null;
//            byte[] temp = null;
//            if(file.exists()){
//                // TODO 从本地读取文件
//                temp = Utils.getFileBytes(String.valueOf(file));
//                result = Utils.TwoArry(temp);
//            } else {
//                startKey = recordKey + "-" + String.format("%0" + tempRang + "d", jsonArray.get(i));
//                endKey = recordKey + "-" + String.format("%0" + tempRang + "d", Integer.parseInt(jsonArray.getString(i + 1)));
//                result = atlChain.queryByte(
//                        chaincodeName,
//                        "GetRecordByKeyRange",
//                        new byte[][]{startKey.getBytes(), endKey.getBytes()}
//                );
//                // TODO 增加新方法 先存本地，然后读取
//                Utils.saveByteToLocalFile(result, tmpFile, recordKey, String.valueOf(i));
//            }
//            byteMerger = byteMerger(byteMerger, result, i);
//        }
//        return byteMerger;
//    }

    // TODO 2019.12.19根据提示的范围进行范围查询geometry（startkey包含------endkey不包含）
    public byte[][] getRecordByRange(String recordKey, String chaincodeName, JSONArray jsonArray) {

        int tempRang = jsonArray.get(jsonArray.size() - 1).toString().length() + 2;
        byte[][] byteMerger = null;
        String startKey;
        String endKey;
        for(int i = 0; i < jsonArray.size() -1; i ++){
            startKey = recordKey + "-" + String.format("%0" + tempRang + "d", jsonArray.get(i));
            endKey = recordKey + "-" + String.format("%0" + tempRang + "d", Integer.parseInt(jsonArray.getString(i + 1)));
            byte[][] result = atlChain.queryByte(
                    chaincodeName,
                    "GetRecordByKeyRange",
                    new byte[][]{startKey.getBytes(), endKey.getBytes()}
            );
            byteMerger = byteMerger(byteMerger, result, i);
            result = null;
        }
        return byteMerger;
    }

    // 分页查询 返回的是一页的数据，组合在一起即可  一条一条查没有问题，前面包含，后面不包含
    public byte[][] getStateByRangeWithPagination(String recordKey, String chaincodeName, int pageSize, int allCount) {
        byte[][] byteMerger = null;
        int count = allCount / pageSize +1;
        for(int i = 0; i < count; i++) {
            String startKey = recordKey +  "-" + String.format("%05d", i * pageSize);
            String endKey = recordKey + "-" + String.format("%05d", (i + 1) * pageSize);
            byte[][] result = atlChain.queryByte(
                    chaincodeName,
                    "GetRecordByKeyRange",
                    new byte[][]{startKey.getBytes(), endKey.getBytes()}
            );
//            byteMerger = byteMerger(byteMerger, result, i);
        }
        return byteMerger;
    }

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

    // 删除当前世界状态库的数据
    /**
     * new add 删除当前世界状态库指定键的数据
     */
    public String deleteRecord(String recordKey, String chaincodeName, String functionName) {
        String result = atlChain.invoke(
                chaincodeName,
                functionName,
                new String[]{recordKey}
        );
        return result;
    }

}
