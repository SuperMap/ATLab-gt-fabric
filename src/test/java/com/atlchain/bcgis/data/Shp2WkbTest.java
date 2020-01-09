package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.protoConvert;
import com.atlchain.sdk.ATLChain;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Shp2WkbTest {
    private String shpURL = this.getClass().getResource("/Country_R/Country_R.shp").getFile();//   /Point/Point  /Country_R/Country_R.shp
    private File shpFile = new File(shpURL);
    private Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
    private BlockChainClient client;
    private File networkFile = new File("E:\\DemoRecording\\A_SuperMap\\ATLab-gt-fabric\\src\\test\\resources\\network-config-test.yaml");
    public Shp2WkbTest(){
        client = new BlockChainClient(networkFile);
    }

    @Test
    public void testGetRightGeometryCollectionType() {
        Assert.assertEquals(ArrayList.class, shp2WKB.getGeometry().getClass());
    }

    @Test
    public void testGetRightGeometryValue() {
        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
//            for(Geometry geom : geometryArrayList) {
//                System.out.println(geom);
//            }
        Assert.assertNotNull(geometryArrayList);
    }

    @Test
    public void testSaveWKB(){
        String path = "E:\\SuperMapData\\China\\Province_R.wkb";
        shp2WKB.save(new File(path));
//            Assert.assertTrue(Files.exists(Paths.get(path)));
    }

    /**
     * 将 shp 数据整个以二进制的方式存取区块链
     */
    @Test
    public void testSaveGeometryToChain() {
//        String key =  "D";
//        byte[] bytes = shp2WKB.getGeometryBytes();
//        String result = client.putRecord(
//                key,
//                bytes,
//                "bcgiscc",
//                "PutRecordBytes"
//        );
//        System.out.println(result);
    }

    /**
     *  根据 键 读取区块链上的二进制 gemetry
     * @throws ParseException
     */
    @Test
    public void testQueryGeometryFromChain() throws ParseException {
        String key = "b6a5833aba1f3a73e9d721a6df15defd00b17a3722491bb33b7700d37f288d5b-197585";
        byte[][] result = client.getRecordBytes(
                key,
                "bcgiscc",
                "GetRecordByKey"
        );
        System.out.println(result);

        // 单个可以读取出来
        JSONObject jsonProp = protoConvert.getPropFromProto(result[0]);
        System.out.println(jsonProp);
        Geometry geometry = protoConvert.getGeometryFromProto(result[0]);
        System.out.println(geometry);

//        Geometry geometry = Utils.getGeometryFromBytes(result[0]);
//        System.out.println(geometry);
//        System.out.println(geometry.getNumGeometries());
    }

    /**
     * 根据 hashID 查询数据
     */
    @Test
    public void testQueryFromChain(){
        String key = "prop6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-";
        for(int i = 0; i < 99; i++){
            String strIndex = key + String.format("%04d", i);
            String value = client.getRecord(strIndex,"bcgiscc");
            System.out.println(value);
        }
    }


    // 存值
    @Test
    public void testSaveToChain(){
        String key =  "testKey";
        String value = "testValue";
        String result = client.putRecord(
                key,
                value,
                "bcgiscc",
                "PutRecordBytes"
        );
        System.out.println(result);
    }

    // 查值
    @Test
    public void testQueryToChain(){
        String key =  "testKey";
        String queryString = client.getRecord(
                key,
                "bcgiscc",
                "GetRecordByKey"
        );
        System.out.println(queryString);
    }

    // 删值
    @Test
    public void testDelte(){
        String key =  "testKey";
        String deleteString = client.deleteRecord(
                key,
                "bcgiscc",
                "DeleteRecordByKey"
        );
        System.out.println(deleteString);

    }

    // 循环删除值
    @Test
    public void testDeleteByKey(){
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-";
        for(int i = 0; i < 100; i++) {
            String strIndex = key + String.format("%04d", i);
            String recordKey = key  + strIndex;
            String result = client.deleteRecord(
                    recordKey,
                    "bcgiscc",
                    "DeleteRecordByKey"
            );
            System.out.println(result + "====>>>>>" + strIndex);
        }
    }

    @Test
    public void testGeoJSON(){

        String shpURL = "E:\\SuperMapData\\hu.json";
        File geoJsonFile = new File(shpURL);
        String s = Utils.readJsonFile(String.valueOf(geoJsonFile));
        System.out.println(s);
//        JSONArray jsonArray = (JSONArray)JSON.parse(s);
    }

    @Test
    public void test22(){
        shp2WKB.getShpFileAttributes();
    }

    // CouchDB富查询测试（需在外部构建索引查询）
    @Test
    public void testQueryProp(){
        String queryKey1 = "hash";
        String queryValue1 = "23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8-0000";
        String queryKey2 = "Name";
        String queryValue2 = "aomen";
        String queryKey3 = "hashIndex";
        String queryValue3 = "23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8";

        JSONObject json = new JSONObject();
//        json.put(queryKey1, queryValue1);
//        json.put(queryKey2, queryValue2);
        json.put(queryKey3, queryValue3);

//        String  selector = "{\"" + queryKey1 + "\":\"" + queryValue1 + "\"}";
//        String  selector = "{\"hash\":\"" + queryValue1 + "\"}";
//        stub.getQueryResult("{\"selector\":{\"Name\":\"" + name + "\", \"mapName\":\"" + mapName + "\"}}");
        Set<String> keys = json.keySet();
        StringBuilder qureySelector = new StringBuilder();
        Boolean start = false;
        int count = 1;
        int total = keys.size();
        qureySelector.append("{\"" );
        for(String key : keys){
            if(start){
                qureySelector.append(",");
                qureySelector.append("\"");
            }
            qureySelector.append(key);
            qureySelector.append("\":\"");
            qureySelector.append(json.getString(key));
            if(count < total){
                qureySelector.append("\"");
            }
            start = true;
            count = count + 1;
        }
        qureySelector.append("\"}");
        String selector1 = qureySelector.toString();
        System.out.println(selector1);
        String queryResult = client.getRecordBySeletor(
                "bcgiscc",
                "GetRecordBySelector",
                selector1
        );
        System.out.println(queryResult);
    }

}
































