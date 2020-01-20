package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONObject;
import com.supermap.blockchain.sdk.SmChain;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;

public class Shp2WkbTest {

    private SmChain smChain;
    private BlockChainClient client;
    private File networkFile = new File("E:\\DemoRecording\\A_SuperMap\\ATLab-gt-fabric\\src\\test\\resources\\network-config-test.yaml");
    private String channelName = "txchannel";

    private String shpURL = this.getClass().getResource("/D/D.shp").getFile();//   /Point/Point  /Country_R/Country_R.shp
    private File shpFile = new File(shpURL);
    private Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
    public Shp2WkbTest(){
        smChain = SmChain.getSmChain(channelName, networkFile);
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
        String path = "E:\\SuperMapData\\Province_R.wkb";
        shp2WKB.save(new File(path));
//            Assert.assertTrue(Files.exists(Paths.get(path)));
    }

    /**
     * 测试存 byte 数组
     */
    @Test
    public void testSaveGeometryToChain() {
        String key =  "D";
        byte[] bytes = key.getBytes();
        String s = smChain.getSmTransaction().invokeByte(
                "bcgiscc",
                "PutRecordBytes",
                new byte[][]{ key.getBytes(), bytes}
        );
        System.out.println(s);
    }

    /**
     *  根据 键 读取区块链上的二进制 gemetry
     * @throws ParseException
     */
    @Test
    public void testQueryGeometryFromChain() throws ParseException {
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-0002";
        byte[][] result1 = smChain.getSmTransaction().queryByte(
                "bcgiscc",
                "GetRecordByKey",
                new byte[][] {key.getBytes()}
        );
        Geometry geometry = Utils.getGeometryFromBytes(result1[0]);
//        System.out.println(geometry);
        System.out.println(geometry.getNumGeometries());
    }

    // 存值
    @Test
    public void testSaveToChain(){
        String key =  "testKey";
        String value = "testValue";
        String result = smChain.getSmTransaction().invoke(
                "bcgiscc",
                "PutRecordBytes",
                new String[] {key, value}
        );
        System.out.println(result);
    }

    // 查值
    @Test
    public void testQueryToChain(){
        String key =  "testKey";
        String queryString = smChain.getSmTransaction().query(
                "bcgiscc",
                "GetRecordByKey",
                new String[] {key}
        );
        System.out.println(queryString);
    }

    // 删值
    @Test
    public void testDelte(){
        String key =  "testKey";
        String deleteString = smChain.getSmTransaction().invoke(
                "bcgiscc",
                "DeleteRecordByKey",
                new String[] {key}
        );
        System.out.println(deleteString);

    }

    @Test
    public void testGetShpFileAttributes(){
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
































