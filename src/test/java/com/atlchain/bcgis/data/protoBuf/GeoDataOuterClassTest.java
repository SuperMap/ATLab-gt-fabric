package com.atlchain.bcgis.data.protoBuf;

import com.alibaba.fastjson.JSONArray;
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

    private String shpURL = this.getClass().getResource("/D/D.shp").getFile(); //  /Province/Province_R.shp  /D/D.shp
    private File shpFile = new File(shpURL);
    Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);

    /**
     * proto 数据的序列化与反序列化测试
     */
    @Test
    public void testProto(){

        ArrayList<Geometry> geometryArrayList = shp2WKB.getGeometry();
        JSONArray jsonProps = shp2WKB.getShpFileAttributes();

        for (int i = 0; i < geometryArrayList.size(); i ++) {
            // 模拟将将空间几何数据（geometry）和属性（jsonObject）以 proto 方式转为 bytes
            JSONObject jsonProp0 = JSONObject.parseObject(jsonProps.get(i).toString());
            Geometry geometry0 = geometryArrayList.get(i);
            byte[] bytes =  protoConvert.dataToProto(geometry0, jsonProp0);

            // 解析 bytes 得到 geometry 和 属性
            JSONObject jsonProp = protoConvert.getPropFromProto(bytes);
            System.out.println(jsonProp);
            Geometry geometry = protoConvert.getGeometryFromProto(bytes);
            System.out.println(geometry);
        }
    }
}