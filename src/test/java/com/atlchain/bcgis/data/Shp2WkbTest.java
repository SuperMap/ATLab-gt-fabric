package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.protoConvert;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Shp2WkbTest {
    private String shpURL = this.getClass().getResource("/D/D.shp").getFile();//   /Point/Point
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
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-00011";
        byte[][] result = client.getRecordBytes(
                key,
                "bcgiscc",
                "GetRecordByKey"
        );

        // 单个可以读取出来
        JSONObject jsonProp = protoConvert.getPropFromProto(result[0]);
        System.out.println(jsonProp);
        Geometry geometry = protoConvert.getGeometryFromProto(result[0]);
        System.out.println(geometry);

//        Geometry geometry = Utils.getGeometryFromBytes(result[0]);
//        System.out.println(geometry);
//        System.out.println(geometry.getNumGeometries());
    }

    //   D             6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4
    //  中国地图        23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8
    /**
     * 根据 hashID 查询数据
     */
    @Test
    public void testQueryFromChain(){
        String key = "23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8-00034";
        String value = client.getRecord(key,"bcgiscc");
        System.out.println(value);
    }

    /**
     * 直接指定 键-值 存入到区块链
     */
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

    //   D   6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4     attributes
    /**
     * 测试删除指定键值
     */
    @Test
    public void testDeleteByKey(){
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-";
        for(int i = 0; i < 50; i++) {
            String strIndex = String.format("%06d", i);
            String attributes = "attributes";
            String recordKey = key  + strIndex;
            String result = client.deleteRecord(
                    recordKey,
                    "bcgiscc",
                    "DeleteRecordByKey"
            );
        }
    }

    @Test
    public void testSha256() {
        String sha256 = Utils.getSHA256("bbb");
        Assert.assertEquals("3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677", sha256);
    }

    @Test
    public void testGeoJSON(){

        String shpURL = "E:\\SuperMapData\\hu.json";
        File geoJsonFile = new File(shpURL);
        String s = Utils.readJsonFile(String.valueOf(geoJsonFile));
        System.out.println(s);
//        JSONArray jsonArray = (JSONArray)JSON.parse(s);
    }

}