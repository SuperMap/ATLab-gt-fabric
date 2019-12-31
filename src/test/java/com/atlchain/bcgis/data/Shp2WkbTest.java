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
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-0000175109";
        String value = client.getRecord(key,"bcgiscc");
        System.out.println(value);
        for(int i = 0; i < 99; i++){
            System.out.println(String.format("%0" + String.valueOf(5).length() + "d", i));
        }
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

    //   D               6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4
    //  BL              d7e94bf0c86c94579e8b564d2dea995ed3746108f98f003fb555bcd41831f885
    //  P               b6a5833aba1f3a73e9d721a6df15defd00b17a3722491bb33b7700d37f288d5b
    /**
     * 测试删除指定键值
     */
    @Test
    public void testDeleteByKey(){
        String key = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4-";
        for(int i = 0; i < 100; i++) {
            String strIndex = String.format("%04d", i);
            System.out.println(strIndex);
            String recordKey = key  + strIndex;
            String result = client.deleteRecord(
                    recordKey,
                    "bcgiscc",
                    "DeleteRecordByKey"
            );
//            System.out.println(result + "====================》》》》》" + i);
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



    @Test
    public void test44(){
        int t = 1111111;
        String s = String.format("%010d", t);
        System.out.println(s);
    }

    @Test
    public void test22(){
        shp2WKB.getShpFileAttributes();
    }

}