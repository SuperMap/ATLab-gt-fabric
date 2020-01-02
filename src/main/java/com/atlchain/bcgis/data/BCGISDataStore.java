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
    private byte[][] geometryResults ;
    private byte[][] propResults ;

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
            this.geometryResults = getGeometryDataFromChaincode();
            this.propResults = getPropDataFromChaincode();
        } else {
            try {
                this.recordKey = putDataOnBlockchainByProto();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  数据存入区块链
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

        // 编写整体信息  整体信息最后存储
        String result = "";
        Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
        JSONArray geometryProperty = shp2WKB.getShpFileAttributes();
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
        if(result1.length() == 0){
            JSONObject jsonObject = (JSONObject)JSON.parse(result1);
            String hash1 = (String) jsonObject.get("hash");
            if(hash1.equals(key)) {
                return key;
            }
        } else {
            // 存放单条空间几何记录 自定义范围查询（一组范围最多800条或累计超800KB），范围存储在 JSONArray
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(0);
            int tmpCount = 0;                 // 用来计数最大为 900
            int DataSize = 0;                 // 表示单条范围内最多的数据
            int maxLimit = 800;              // 一次最多存储的个数  定900不行
            // 单个geometry的键值设计 "key" + "-" + String.format("%0" + tempRang + "d", i);
            int rang = geometryArrayList.size();
            int tempRang = String.valueOf(rang).length() + 2;
            int max = 0;
            for (int i = 0; i < rang; i++) {
                Geometry geometry = geometryArrayList.get(i);
                byte[] geoBytes = Utils.getBytesFromGeometry(geometry);
                int min = geoBytes.length / 1024;
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
            logger.info("空间几何信息存储完毕，单条数据最大值为" + max + "KB");

            // 存储单条geometry属性信息
            for(int i = 0; i < geometryProperty.size(); i++ ) {
                String propKey = "prop" + key + "-" + String.format("%0" + tempRang + "d", i);
                String contactKey = key + "-" + String.format("%0" + tempRang + "d", i);
                JSONObject json = (JSONObject) geometryProperty.get(i);
                json.put("hash", contactKey);
                String propValue = json.toJSONString();
                result = client.putRecord(
                        propKey,
                        propValue,
                        chaincodeName,
                        "PutRecord"
                );
                if (!result.contains("successfully")) {
                    return "Put data on chain FAILED! MESSAGE:" + result;
                }
            }
            logger.info("属性信息存储完毕");

            // 将该对象拥有的属性字段也存储到整体信息
            JSONArray geoProperty = shp2WKB.getShpFileAttributes();
            JSONObject jsonProp = (JSONObject)geoProperty.get(0);
            Set<String> keys =  jsonProp.keySet();
            JSONArray Prop = new JSONArray();
            for(String s : keys){
                Prop.add(s);
            }
            argsJson.put("prop", Prop);
            argsJson.put("rang", tempRang);
            // 存储整体信息
            jsonArray.add(rang);
            argsJson.put("readRange", jsonArray);
            String args = argsJson.toJSONString();
            result = client.putRecord(
                    key,
                    args,
                    chaincodeName,
                    "PutRecord"
            );
            if (!result.contains("successfully")) {
                return "Put data on chain FAILED! MESSAGE:" + result;
            }
            logger.info("整体信息存储完毕");
        }
        return result;
    }

    /**
     * 以 proto 的格式将数据存入到区块链（后面会删除）
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
     * 空间数据查询
     * @return
     */
    public byte[][]  getGeometryDataFromChaincode(){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("readRange").toString());
        // 新的范围查询(自定义分页进行查询)
        geometryResults = client.getRecordByRange(
                this.recordKey,
                this.chaincodeName,
                jsonArray
        );
        logger.info("getGeometryDataFromChaincode is success");
        return geometryResults;
    }

    /**
     * 属性查询
     * @return
     */
    public byte[][]  getPropDataFromChaincode(){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("readRange").toString());
        String key = "prop" + this.recordKey;
        propResults = client.getRecordByRange(
                key,
                this.chaincodeName,
                jsonArray
        );
        logger.info("getPropDataFromChaincode is success");
        return propResults;
    }

    /**
     * 单个属性依靠 hash 查询 ==============>这种方式太慢了
     * @param index
     * @return
     */
    public JSONObject getSinglePropFromChainCode(int index){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        String rang = jsonObject.get("rang").toString();
        String propKey = "prop" + this.recordKey+ "-" + String.format("%0" + rang + "d", index);
        String prop = client.getRecord(
                propKey,
                this.chaincodeName
        );
        JSONObject jsonProp = JSONObject.parseObject(prop);
        return jsonProp;
    }

    /**
     * 获取空间几何数据(分页读取机制)
     * 因为数据和属性都是采用相同的分页读取，所以可以保持一致得到属性和数据时对应的
     * @return
     */
    public Geometry getRecord(int page){
        Geometry geometryTmp = null;
//        byte[][] geometryResults = getGeometryDataFromChaincode();
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();
        int pageCount = 0;
        for (byte[] resultByte : geometryResults) {
            // 确定好是那一页才进去解析 解析完毕直接退出，不在循环
            if(page == pageCount) {
                String resultStr = new String(resultByte);
                JSONArray jsonArray = (JSONArray) JSON.parse(resultStr);
                for (Object obj : jsonArray) {
                    JSONObject jsonObj = (JSONObject) obj;
                    String recordBase64 = (String) jsonObj.get("Record");
                    byte[] bytes = Base64.getDecoder().decode(recordBase64);
                    try {
                        geometryTmp = new WKBReader().read(bytes);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    geometryArrayList.add(geometryTmp);
                    geometryTmp = null;
                }
                break;
            }
            pageCount++;
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
//        logger.info("完成空间几何信息解析");
        return geometryCollection;
    }

    /**
     * 获取空间几何数据
     * @return
     */
    public Geometry getRecordByProto(){
        logger.info("开始空间几何信息解析");
        Geometry geometry = null;
        byte[][] results = getGeometryDataFromChaincode();
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
        logger.info("完成几何信息解析，总共" + geometryCollection.getNumGeometries() + "条");
        return geometryCollection;
    }

    /**
     * 整体属性查询（分页解析数据）
     * @return
     */
    public JSONArray getProperty(int page){
//        logger.info("开始属性查询");
        JSONArray jsonArrayProp = new JSONArray();
        int pageCount = 0;
        for (byte[] resultByte : propResults) {
            // 做判断 分页解析数据
            if(pageCount == page) {
                String resultStr = new String(resultByte);
                JSONArray jonArray = (JSONArray) JSON.parse(resultStr);
                for (Object obj : jonArray) {
                    JSONObject jsonObj = (JSONObject) obj;
                    String recordBase64 = (String) jsonObj.get("Record");
                    byte[] bytes = Base64.getDecoder().decode(recordBase64);
                    String tmpString = new String(bytes);
                    JSONObject json = JSONObject.parseObject(tmpString);
                    json.remove("hash");
                    // 为节省空间，只将值传输给那边解析即可，键有顺序就行
                    Set<String> keys = json.keySet();
                    JSONArray jsonArrayTmp = new JSONArray();
                    for (String propKey : keys) {
                        String value = json.getString(propKey);
                        if (value.length() == 0) {
                            jsonArrayTmp.add("null");
                        } else {
                            jsonArrayTmp.add(value);
                        }
                    }
                    jsonArrayProp.add(jsonArrayTmp);
                    jsonArrayTmp = null;
                }
                break;
            }
            pageCount++;
        }
//        logger.info("属性解析完毕");
        return jsonArrayProp;
    }

    /**
     * 获取全部的属性值
     * 现在的方式是先读取保存为文件，所以属性时直接读取文件即可
     * 获取属性
     * @return
     */
    public JSONArray getPropertyByProto(){

        byte[][] results = getGeometryDataFromChaincode();
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
                // 只得到属性值有顺序即可
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
        }
        if (jsonArrayProp == null) {
            try {
                throw new IOException("Blockchain record prop defalut");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("完成属性信息解析，总共" + jsonArrayProp.size()  + "条");
        return jsonArrayProp;
    }


    /**
     * 获取属性信息字段 和整体类型
     * @return
     */
    public List<Object> getPropertynName(){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        List<Object> list = new LinkedList<>();
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        String geotype = jsonObject.get("geotype").toString();
        JSONArray jsonArray = (JSONArray) jsonObject.get("prop");
        list.add(geotype);
        list.add(jsonArray);
        return list;
    }

    // 获取存储的数据有多少个 和 分页情况
    public List<Object> getCount(){
        String result = client.getRecord(
                this.recordKey,
                this.chaincodeName,
                this.functionName
        );
        if(result.length() == 0){
            logger.info("please input correct recordKey");
        }
        JSONObject jsonObject = (JSONObject)JSON.parse(result);
        int totalCount = (int)jsonObject.get("count");
        JSONArray jsonArrayReadRange = JSONArray.parseArray(jsonObject.get("readRange").toString());
        List<Object> list = new LinkedList<>();
        list.add(totalCount);
        list.add(jsonArrayReadRange);

        return list;
    }

    /**
     * TODO 富查询做属性查询（后续再改）
     * 以属性值和总的hash值作为查询手段，然后得到该属性的hash，在进行一次查询
     * @param stringList
     * @return
     */
    public Geometry queryAttributes(List<String> stringList){
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
     * 基于proto格式的属性查询，现在有富查询之后，可能不在使用
     * @param jsonObject
     * @return
     */
    public Geometry queryAttributesByProto(JSONObject jsonObject) {

        JSONArray jsonArrayProp = getProperty(0);
        Geometry geometry = getRecord(0);
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
        Name name = new NameImpl(namespaceURI, this.recordKey);
        return Collections.singletonList(name);
    }

    @Override
    public ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new BCGISFeatureStore(entry, Query.ALL);
    }
}










