package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.protoConvert;
import com.google.common.io.Files;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.opengis.feature.type.Name;

import javax.json.JsonArray;
import java.io.File;
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
    private byte[][] results ;

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
            this.results = getDataFromChaincode();
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

        // TODO 这里设计一个范围查询的列表以 JSONArray 的形式存储，代表着查询的范围，根据每一范围的数据量最多为 900 和每一范围最大为 1024 kb
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(0);

        int tmpCount = 0;               // 用来计数最大为 900
        int DataSize = 0;         // 用来表示打次范围内最多的数据
        // 存放单条记录
        JSONArray jsonProps = shp2WKB.getShpFileAttributes();
        // TODO 单个geometry的存放方式是总的对象数前面再加2个零即可
        int rang = geometryArrayList.size();
        int tempRang = String.valueOf(rang).length() + 2;
        for (int i = 0; i < rang; i ++) {
            JSONObject jsonProp = JSONObject.parseObject(jsonProps.get(i).toString());
            Geometry geometry = geometryArrayList.get(i);
            byte[] geoBytes =  protoConvert.dataToProto(geometry, jsonProp);

            // TODO 添加自动分页的存储机制 jsonArray 在读取中使用
            DataSize = DataSize + geoBytes.length;
            System.out.println("这是第" + i + "个" + geoBytes.length / 1024);
            tmpCount = tmpCount + 1;
            if(DataSize > 800 * 800 || tmpCount > 900){
                jsonArray.add(i);
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

        jsonArray.add(geometryArrayList.size()); // 范围完成 从 0 开始 到最后结束
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
        return key;
    }

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
        results = client.getRecordByRange(
                this.recordKey,
                this.chaincodeName,
                jsonArray
        );
        System.out.println("getDataFromChaincode");
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

        Geometry geometry;
//        byte[][] results = getDataFromChaincode();
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
        // TODO 现在数据比较混乱，暂时不加这个
//        if( count != geometryCollection.getNumGeometries()){
//            return null;
//        }
        return geometryCollection;
    }

    /**
     * 获取属性
     * @return
     */
    public JSONArray getProperty(){

//        byte[][] results = getDataFromChaincode();
        JSONObject jsonProp;
        JSONArray jsonArrayProp = new JSONArray();
        for (byte[] resultByte : results) {
            String resultStr = new String(resultByte);
            JSONArray jsonArray = (JSONArray)JSON.parse(resultStr);
            for (Object obj : jsonArray){
                JSONObject jsonObj = (JSONObject) obj;
                String recordBase64 = (String)jsonObj.get("Record");
                byte[] bytes = Base64.getDecoder().decode(recordBase64);
                jsonProp = protoConvert.getPropFromProto(bytes);
                jsonArrayProp.add(jsonProp);
            }
        }

        if (jsonArrayProp == null) {
            try {
                throw new IOException("Blockchain record prop defalut");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // TODO 现在数据比较混乱，暂时不加这个
//        if( count != jsonArrayProp.size() ){
//            return null;
//        }
        return jsonArrayProp;
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
     * 通过解析 jsonArrayProp 查询对应的属性，然后得到序号，最后得到相应属性的 geometry
     * TODO 这只是一个方法，后期需要的话再把 key 加进来就可以了
     * @param jsonObject 包含查询的多个 key-->value
     * @return
     */

    /**
     * 解析得到geometry和proto，然后根据查询条件解析属性得到对应的geometry
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
        Name name = new NameImpl(namespaceURI, this.recordKey);
        return Collections.singletonList(name);
    }

    @Override
    public ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {

        return new BCGISFeatureStore(entry, Query.ALL);
    }
}










