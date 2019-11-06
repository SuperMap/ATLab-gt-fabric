package com.atlchain.bcgis.data;

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
//    private String shpURL = this.getClass().getResource("D/D.shp").getFile();//   /Point/Point
    private String shpURL = "/home/cy/Documents/ATL/SuperMap/ATLab-BCGIS/gt-bcgis/src/test/resources/D/D.shp";
    private File shpFile = new File(shpURL);
    private Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
    private BlockChainClient client;
//    private File networkFile = new File(this.getClass().getResource("network-config-test.yaml").toURI());
    private File networkFile = new File("/home/cy/Documents/ATL/SuperMap/ATLab-BCGIS/gt-bcgis/src/test/resources/network-config-test.yaml");

    public Shp2WkbTest() throws URISyntaxException {
        client = new BlockChainClient(networkFile);
    }

    @Test
    public void testGetRightGeometryCollectionType() {
        try {
            Assert.assertEquals(ArrayList.class, shp2WKB.getGeometry().getClass());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetRightGeometryValue() {
        try {
            ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
//            for(Geometry geom : geometryArrayList) {
//                System.out.println(geom);
//            }
            Assert.assertNotNull(geometryArrayList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveWKB(){
        try {
            String path = "E:\\DemoRecording\\WkbCode\\Line.wkb";
            shp2WKB.save(new File(path));
            Assert.assertTrue(Files.exists(Paths.get(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveGeometryToChain() throws IOException {
        String key =  "D";
        byte[] bytes = shp2WKB.getGeometryBytes();

        String result = client.putRecord(
                key,
                bytes,
                "bcgiscc",
                "PutRecordBytes"
        );
        System.out.println(result);
    }

    @Test
    public void testQueryGeometryFromChain() throws ParseException {
        String key = "D";
        byte[][] result = client.getRecordBytes(
                key,
                "bcgiscc",
                "GetRecordByKey"
        );
        Geometry geometry = Utils.getGeometryFromBytes(result[0]);
        System.out.println(geometry.getNumGeometries());
    }

    // 为区块链存证系统服务
    @Test
    public void testSaveToChain() throws IOException {
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

    @Test
    public void testQueryFromChain() throws ParseException {
        String key = "131102taochengqu";
        String value = client.getRecord(key, "131100",  "bcgiscc");
        System.out.println(value);
    }

    @Test
    public void testQueryGeometryFromChain1() throws ParseException {
        BlockChainClient client2 = new BlockChainClient(networkFile);
        for(int i = 0; i < 10000; i++) {
            String key = "Line4";
            byte[][] result = client2.getRecordBytes(
                    key,
                    "bcgiscc",
                    "GetRecordByKey"
            );
            Geometry geometry = Utils.getGeometryFromBytes(result[0]);
            System.out.println(geometry.getNumGeometries()+"======" + i);
        }
    }

    @Test
    public void testSha256() {
        String sha256 = Utils.getSHA256("bbb");
        Assert.assertEquals("3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677", sha256);
    }

    @Test
    public void testThread() throws URISyntaxException {
        for (int i=0; i< 1000 ; i++) {
            Utils.ThreadDemo threadDemo = new Utils.ThreadDemo("测试Thread" + i);
//            threadDemo.start();
            threadDemo.run();
        }
    }
}