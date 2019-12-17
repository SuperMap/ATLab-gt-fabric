package com.atlchain.bcgis.data.protoBuf;

import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.Shp2Wkb;
import com.atlchain.bcgis.data.Utils;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class GeoDataOuterClassTest {

    private String shpURL = this.getClass().getResource("/D/D.shp").getFile();
    private File shpFile = new File(shpURL);
    Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);

    @Test
    public void testProto(){
        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key1", "value1");
        jsonObject.put("key2", "value2");
        jsonObject.put("key3", "value3");
        jsonObject.put("key4", "value4");
        for (Geometry geo : geometryArrayList) {
            // 模拟将将空间几何数据（geometry）和属性（jsonObject）以 proto 方式转为 bytes
            byte[] bytes =  protoConvert.dataToProto(geo, jsonObject);
            // 解析 bytes 得到 geometry 和 属性
            JSONObject jsonProp = protoConvert.getPropFromProto(bytes);
            System.out.println(jsonProp);
            Geometry geometry = protoConvert.getGeometryFromProto(bytes);
            System.out.println(geometry);
            return;
        }
    }




















}