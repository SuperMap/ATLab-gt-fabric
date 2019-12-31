package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.protoConvert;
import com.google.common.io.Files;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.geotools.data.DataSourceException;
import org.geotools.data.Query;
import org.geotools.data.shapefile.shp.ShapeType;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class BCGISDataStore extends ContentDataStore {

    Logger logger = Logger.getLogger(BCGISDataStore.class.toString());

    private String chaincodeName;
    private String functionName;
    private String recordKey;
    private BlockChainClient client;
    private File shpFile;
    private int count;
//    private byte[][] results ;

    public BCGISDataStore(
            File networkConfigFile,
            File shpFile,
            String chaincodeName,
            String functionName,
            String recordKey
    )
    {
        this.chaincodeName = chaincodeName;
        this.functionName = functionName;
        client = new BlockChainClient(networkConfigFile);
        this.shpFile = shpFile;
        if( !recordKey.equals("null") ){
            this.recordKey = recordKey; // 获取外界的  key 进行发布  先判断，假如有的话那就不再计算了
//            this.results = getDataFromChaincode();
        } else {
            try {
                this.recordKey = putDataOnBlockchainByProto();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * TODO 之前的存入区块链的方法，后面需要删除
     * @param shpFile
     * @return
     * @throws IOException
     */
    public String putDataOnBlockchain(File shpFile) throws IOException {
        String fileName = shpFile.getName();

        String ext = Files.getFileExtension(fileName);
        if(!"shp".equals(ext)) {
            throw new IOException("Only accept shp file");
        }

        String result = "";
        Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
        System.out.println("===================>>>>" + geometryArrayList.size());
        String geometryStr = Utils.getGeometryStr(geometryArrayList);
        String hash = Utils.getSHA256(geometryStr);
        String key = hash;
//        System.out.println(key);
        String mapname = fileName.substring(0, fileName.lastIndexOf("."));

        JSONObject argsJson = new JSONObject();
        argsJson.put("mapname", mapname);
        argsJson.put("count", geometryArrayList.size());
        argsJson.put("hash", key);
        argsJson.put("geotype", geometryArrayList.get(0).getGeometryType());
        argsJson.put("PID", "");
        String args = argsJson.toJSONString();
        //存放整个的记录，读取时采用按范围索引
        result = client.putRecord(
                key,
                args,
                chaincodeName,
                "PutRecord"
        );
        if (!result.contains("successfully")) {
            return "Put data on chain FAILED! MESSAGE:" + result;
        }

        // 存放属性值
        JSONArray jsonArrayAttributes = shp2WKB.getShpFileAttributes();
        for(int i = 0; i < jsonArrayAttributes.size(); i++ ){
            JSONObject JsonAttributes = new JSONObject();
            String attributesKey = "attributes" + key +  "-" + String.format("%05d", i);
            JsonAttributes.put("Name", jsonArrayAttributes.get(i));
            JsonAttributes.put("mapName", hash);
            String Attributes = JsonAttributes.toJSONString();
            result = client.putRecord(
                    attributesKey,
                    Attributes,
                    chaincodeName,
                    "PutRecord"
            );
            if (!result.contains("successfully")) {
                return "Put data on chain FAILED! MESSAGE:" + result;
            }
        }

        // 存放单条记录
        int index = 0;
        for (Geometry geo : geometryArrayList) {
            byte[] geoBytes = Utils.getBytesFromGeometry(geo);
            String strIndex = String.format("%05d", index);
            String recordKey = key + "-" + strIndex;
            result = client.putRecord(
                    recordKey,
                    geoBytes,
                    chaincodeName,
                    "PutRecordBytes"
            );
            index++;
            if (!result.contains("successfully")) {
                return "Put data on chain FAILED! MESSAGE:" + result;
            }
            //            Thread.sleep(1000);
        }
        return result;
    }

    /**
     * 以 proto 的格式将数据存入到区块链
     * @return
     * @throws IOException
     */
    public String putDataOnBlockchainByProto() throws IOException {
        String fileName = shpFile.getName();
        String ext = Files.getFileExtension(fileName);
        if(!"shp".equals(ext)) {
            throw new IOException("Only accept shp file");
        }

        String result = "";
        Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
        String geometryStr = Utils.getGeometryStr(geometryArrayList);
        String hash = Utils.getSHA256(geometryStr);
        String key = hash;
        System.out.println(key);
        String mapname = fileName.substring(0, fileName.lastIndexOf("."));

        JSONObject argsJson = new JSONObject();
        argsJson.put("mapname", mapname);
        argsJson.put("count", geometryArrayList.size());
        argsJson.put("hash", key);
        argsJson.put("geotype", geometryArrayList.get(0).getGeometryType());
        argsJson.put("PID", "");
        // 做判断，先根据hash查询整体信息，如果 hash 相同，则说明已经存储过，就不需要在存储，直接返回key
        String result1 = client.getRecord(
                key,
                this.chaincodeName,
                this.functionName
        );
        if(result1.length() != 0){
            JSONObject jsonObject = (JSONObject)JSON.parse(result1);
            String hash1 = (String) jsonObject.get("hash");
            if(hash1.equals(key)) {
                return key;
            }
        } else {
            // 自定义范围查询（一组范围最多800条或累计超800KB），范围存储在 JSONArray
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(0);

            int tmpCount = 0;                 // 用来计数最大为 900
            int DataSize = 0;                 // 表示单条范围内最多的数据
            int maxLimit = 800;              // 一次最多存储的个数  定900不行
            // 存放单条记录
            JSONArray jsonProps = shp2WKB.getShpFileAttributes();
            // 单个geometry的键值设计 "key" + "-" + String.format("%0" + tempRang + "d", i);
            int rang = geometryArrayList.size();
            int tempRang = String.valueOf(rang).length() + 2;

            int max = 0;
            int min = 0;
            for (int i = 0; i < rang; i++) {
                JSONObject jsonProp = JSONObject.parseObject(jsonProps.get(i).toString());
                Geometry geometry = geometryArrayList.get(i);
                byte[] geoBytes = protoConvert.dataToProto(geometry, jsonProp);
                // 计算单条数据最大值
                min = geoBytes.length / 1024;
                if (min > max) {
                    max = min;
                }
                // 自动分页的存储机制 jsonArray 在读取中使用
                DataSize = DataSize + geoBytes.length;
                tmpCount = tmpCount + 1;
                if (DataSize > 1024 * maxLimit || tmpCount > 900) {
                    jsonArray.add(i);
                    System.out.println("分页大小为" + (DataSize - geoBytes.length) / 1024 + "KB");
                    DataSize = 0;
                    tmpCount = 0;
                }

                String strIdex = String.format("%0" + tempRang + "d", i);
                String recordKey = key + "-" + strIdex;
                result = client.putRecord(
                        recordKey,
                        geoBytes,
                        chaincodeName,
                        "PutRecordBytes"
                );
                if (!result.contains("successfully")) {
                    return "Put data on chain FAILED! MESSAGE:" + result;
                }
            }
            logger.info("单条数据最大值为" + max + "KB");
            jsonArray.add(rang);
            argsJson.put("readRange", jsonArray);
            String args = argsJson.toJSONString();
            // 整体信息存储
            result = client.putRecord(
                    key,
                    args,
                    chaincodeName,
                    "PutRecord"
            );
            if (!result.contains("successfully")) {
                return "Put data on chain FAILED! MESSAGE:" + result;
            }
        }
        return key;
    }

    /**
     * 从区块链读取数据，现在的做法是，第一次读取出来就保存到本地，后面继续读取即可
     * 第二次读取的时候直接先判断本地有没有数据，有的话直接返回数据即可
     * @return
     */
    public byte[][]  getDataFromChaincode(){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        // 读取时数据的个数匹配
        count = (int)jsonObject.get("count");
        JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("readRange").toString());
        // TODO 新的范围查询(自定义分页进行查询)
        byte[][] results = client.getRecordByRange(
                this.recordKey,
                this.chaincodeName,
                jsonArray
        );
        logger.info("getDataFromChaincode is success");
        return results;
    }

    /**
     * 之前的获取数据的方式
     * @return
     */
    public Geometry getRecordOld(){

        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );

        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        int count = (int)jsonObject.get("count");

        Geometry geometry = null;
        // TODO 范围查询
        byte[][] results = client.getRecordByRange(
                this.recordKey,
                this.chaincodeName
        );
        // TODO 分页查询
//        int pageSize = 1;
//        byte[][] results = client.getStateByRangeWithPagination(
//                this.recordKey,
//                this.chaincodeName,
//                pageSize,
//                count
//        );

        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        for (byte[] resultByte : results) {
            String resultStr = new String(resultByte);
            JSONArray jsonArray = (JSONArray)JSON.parse(resultStr);
//            if (count != jsonArray.size()) {
//                return null;
//            }
            for (Object obj : jsonArray){
                JSONObject jsonObj = (JSONObject) obj;
                String recordBase64 = (String)jsonObj.get("Record");
                byte[] bytes = Base64.getDecoder().decode(recordBase64);
                try {
                    geometry = new WKBReader().read(bytes);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                geometryArrayList.add(geometry);
            }
        }
        Geometry[] geometries = geometryArrayList.toArray(new Geometry[geometryArrayList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);

        if (geometryArrayList == null) {
            try {
                throw new IOException("Blockchain record is not available");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

//        // TODO 调换读取数据的方式，是地图根据属性进行查询，最后以地图展示
//        List<String> stringList = new ArrayList<>();
//        String key = "beijing";
//        String hash = "23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8";
//        //   D             6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4      属性  D
//        //  中国地图        23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8     属性 省市行政区名
//        stringList.add(key);
//        stringList.add(hash);
//        GeometryCollection geometryCollection = (GeometryCollection) getRecordByAttributes(stringList);
//        System.out.println("===" + geometryCollection);
        return geometryCollection;
    }

    /**
     * 获取空间几何数据
     * @return
     */
    public Geometry getRecord(){
        // TODO 在解析为 几何对象和属性时可不可以直接转为先读取本地文件

        logger.info("开始空间几何信息解析");
        Geometry geometry = null;
        byte[][] results = getDataFromChaincode();
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        for (byte[] resultByte : results) {
            String resultStr = new String(resultByte);
            JSONArray jsonArray = (JSONArray)JSON.parse(resultStr);
            for (Object obj : jsonArray){
                JSONObject jsonObj = (JSONObject) obj;
                String recordBase64 = (String)jsonObj.get("Record");
                byte[] bytes = Base64.getDecoder().decode(recordBase64);
                geometry = protoConvert.getGeometryFromProto(bytes);
                geometryArrayList.add(geometry);
            }
        }
        Geometry[] geometries = geometryArrayList.toArray(new Geometry[geometryArrayList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);
        if (geometryArrayList == null) {
            try {
                throw new IOException("Blockchain record is not available");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if( count != geometryCollection.getNumGeometries()){
            return null;
        }
        logger.info("完成几何信息解析，总共" + geometryCollection.getNumGeometries() + "条");
        return geometryCollection;
    }

    /**
     * TODO 获取全部的属性值
     * 现在的方式是先读取保存为文件，所以属性时直接读取文件即可
     * 获取属性
     * @return
     */
    public JSONArray getProperty(){

        byte[][] results = getDataFromChaincode();
        logger.info("开始属性解析");
        JSONObject jsonProp;
        JSONArray jsonArrayProp = new JSONArray();
        for (byte[] resultByte : results) {
            String resultStr = new String(resultByte);
            JSONArray jsonArray = (JSONArray)JSON.parse(resultStr);
            for (Object obj : jsonArray){
                JSONObject jsonObj = (JSONObject) obj;
                String recordBase64 = (String) jsonObj.get("Record");
                byte[] bytes = Base64.getDecoder().decode(recordBase64);
                // TODO 只得到属性值有顺序即可
                jsonProp = protoConvert.getPropFromProto(bytes);
                // 获取属性字段
                Set<String> keys = jsonProp.keySet();
                JSONArray jsonArrayTmp = new JSONArray();
                for (String key : keys) {
                    String value = jsonProp.getString(key);
                    if (value.length() == 0) {
                        jsonArrayTmp.add("null");
                    } else {
                        jsonArrayTmp.add(value);
                    }
                }
                jsonArrayProp.add(jsonArrayTmp);
                jsonArrayTmp = null;
                jsonProp = null;
            }
//                jsonArrayProp.add(jsonProp);
        }
        if (jsonArrayProp == null) {
            try {
                throw new IOException("Blockchain record prop defalut");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if( count != jsonArrayProp.size() ){
            return null;
        }
        logger.info("完成属性信息解析，总共" + jsonArrayProp.size()  + "条");
        return jsonArrayProp;
    }

    /**
     * TODO 获取属性信息字段
     * @param index
     * @return
     */
    public JSONObject getPropertynName(int index){
        logger.info("获取属性信息字段");
        String tmpFile = "E:\\SuperMapData\\BCGISDataStoreTmpFile";
        File file = new File(tmpFile + File.separator + this.recordKey + String.valueOf(index));
        byte[] temp = Utils.getFileBytes(String.valueOf(file));
        byte[][] results = Utils.TwoArry(temp);
        JSONObject jsonProp = new JSONObject();
        for (byte[] resultByte : results) {
            String resultStr = new String(resultByte);
            JSONArray jsonArray = (JSONArray)JSON.parse(resultStr);
            for (Object obj : jsonArray){
                JSONObject jsonObj = (JSONObject) obj;
                String recordBase64 = (String)jsonObj.get("Record");
                byte[] bytes = Base64.getDecoder().decode(recordBase64);
                jsonProp = protoConvert.getPropFromProto(bytes);
                break;
            }
        }
        logger.info("属性字段获取完毕");
        return jsonProp;
    }

    public String getGeotype(){

        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        // 读取时数据的个数匹配
        String geotype = jsonObject.get("geotype").toString();
        return geotype;
    }

    /**
     * 富查询做属性查询（保留，后期可能会用）
     * 以属性值和总的hash值作为查询手段，然后得到该属性的hash，在进行一次查询
     * @param stringList
     * @return
     */
    public Geometry getRecordByAttributes(List<String> stringList){
        String attributesHash = client.getRecord(
                stringList,
                "bcgiscc",
                "GetAttributesRecordByKey"
        );
        List<String> list = Arrays.asList(attributesHash.split("\n"));
        int stringSize = list.get(0).length();
        int listSize = list.size();
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        for(int i = 0; i < listSize ; i++){
            String S1 = list.get(i);
            String keyA = null;
            if(S1.length() == stringSize){
                keyA = S1.substring(10, stringSize);
            }else {
                continue;
            }
            byte[][] result = client.getRecordBytes(
                    keyA,
                    "bcgiscc",
                    "GetRecordByKey"
            );

            Geometry geometry = null;
            try {
                geometry = Utils.getGeometryFromBytes(result[0]);
                geometryArrayList.add(geometry);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        Geometry[] geometries = geometryArrayList.toArray(new Geometry[geometryArrayList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);
        return geometryCollection;
    }

    /**
     * 解析得到geometry和proto，然后根据查询条件解析属性得到对应的geometry
     * TODO 这只是一个方法，后期需要的话再把 key 加进来就可以了
     * @param jsonObject
     * @return
     */
    public Geometry queryAttributesByProto(JSONObject jsonObject) {

        JSONArray jsonArrayProp = getProperty();
        Geometry geometry = getRecord();
        Geometry geo;
        JSONObject json;
        Set<String> keys = jsonObject.keySet();
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        for(int i = 0; i < jsonArrayProp.size(); i++){
            Boolean judgment = true;
            for(String key : keys){
                String value = jsonObject.getString(key);
                json = JSONObject.parseObject(jsonArrayProp.get(i).toString());
                String queryValue = json.getString(key);
                if( !value.contains(queryValue)){
                    judgment = false;
                    break;//结束本次循环
                }
            }
            if( !judgment){
                continue; // 后面语句不在执行
            }
            geo = geometry.getGeometryN(i);
            geometryArrayList.add(geo);
        }
        Geometry[] geometries = geometryArrayList.toArray(new Geometry[geometryArrayList.size()]);
        GeometryCollection geometryCollection = Utils.getGeometryCollection(geometries);
        return geometryCollection;
    }

    /**
     * 该名字会显示在编辑图层时的命名
     * @return
     */
    @Override
    protected List<Name> createTypeNames() {

//        String tempname = "tempfeaturesType" ;
//        Name name = new NameImpl(namespaceURI, this.recordKey);
//        return Collections.singletonList(name);

        // new add
        return Collections.singletonList(getTypeName());
    }

    Name getTypeName() {
        return new NameImpl(namespaceURI, this.recordKey);
    }

    @Override
    public ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        // new add
        entry = ensureEntry(getTypeName());
        return new BCGISFeatureStore(entry, Query.ALL);
    }
}










