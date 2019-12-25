package com.atlchain.bcgis.data.protoBuf;

import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.Utils;
import com.google.protobuf.ByteString;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * 数据与 proto 的序列化和反序列化
 */
public class protoConvert {

    /**
     * 序列化数据
     * @param geometry      空间几何数据
     * @param jsonObject    属性信息
     * @return
     */
    public static byte[] dataToProto(Geometry geometry, JSONObject jsonObject){

        byte[] bytes = Utils.getBytesFromGeometry(geometry);
        ByteString geometryByte = ByteString.copyFrom(bytes);
        GeoDataOuterClass.GeoData.Builder protoBuilder = GeoDataOuterClass.GeoData.newBuilder();
        protoBuilder.setGeometry(geometryByte);
        Set<String> keys =  jsonObject.keySet();
        for(String key : keys){
            GeoDataOuterClass.Prop.Builder prop = GeoDataOuterClass.Prop.newBuilder();
            prop.setKey(key);
            prop.setValue(jsonObject.get(key).toString());
            protoBuilder.addProp(prop);
        }
        GeoDataOuterClass.GeoData outData = protoBuilder.build();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            outData.writeTo(output);
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] byteArray = output.toByteArray();
        return byteArray;
    }

    /**
     * 反序列化得到 geometry
     * @param byteArray
     * @return
     */
    public static Geometry getGeometryFromProto(byte[] byteArray){
        GeoDataOuterClass.GeoData protoBuilder = parsingProto(byteArray);
        ByteString byteString = protoBuilder.getGeometry();
        byte[] geometryBytes = byteString.toByteArray();
        Geometry geometry = null;
        try {
            geometry = Utils.getGeometryFromBytes(geometryBytes);
            geometryBytes = null;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
        return geometry;
    }

    /**
     * 反序列化得到属性
     * @param byteArray
     * @return
     */
    public static JSONObject getPropFromProto(byte[] byteArray){
        GeoDataOuterClass.GeoData protoBuilder = parsingProto(byteArray);
        List<GeoDataOuterClass.Prop> list = protoBuilder.getPropList();
        JSONObject jsonObject = new JSONObject();
        for(int i = 0; i < list.size(); i++){
            String key = list.get(i).getKey();
            String value = list.get(i).getValue();
            jsonObject.put(key, value);
        }
        return jsonObject;
    }

    private static GeoDataOuterClass.GeoData parsingProto(byte[] byteArray){
        ByteArrayInputStream input = new ByteArrayInputStream(byteArray);
        GeoDataOuterClass.GeoData protoBuilder = null;
        try {
            protoBuilder = GeoDataOuterClass.GeoData.parseFrom(input);
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return protoBuilder;
    }
}
