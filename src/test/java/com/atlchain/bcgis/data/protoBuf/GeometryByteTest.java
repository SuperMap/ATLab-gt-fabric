package com.atlchain.bcgis.data.protoBuf;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GeometryByteTest {


    @Test
    public void testUseProto() throws IOException {

        // 第一、将字节数组 和 属性存入到 proto 中
        ByteString testByte = ByteString.copyFrom("test".getBytes());
        GeoDataOuterClass.GeoData.Builder protoBuilder = GeoDataOuterClass.GeoData.newBuilder();
        protoBuilder.setGeometry(testByte);

        for(int i = 1; i < 5; i++) {
            GeoDataOuterClass.Prop.Builder prop = GeoDataOuterClass.Prop.newBuilder();
            prop.setKey("key_00" + i);
            prop.setValue("value_00" + i);
            protoBuilder.setProp(i, prop);
        }
        GeoDataOuterClass.GeoData outData = protoBuilder.build();
        // 第二、将数据写到输出流，如网络输出流，这里就用ByteArrayOutputStream来代替
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        outData.writeTo(output);

        // <-------------- 分割线：上面是发送方，将数据序列化后发送 --------------->

        byte[] byteArray = output.toByteArray();

        // <-------------- 分割线：下面是接收方，将数据接收后反序列化 --------------->

        // 第三、接收到流并读取，如网络输入流，这里用ByteArrayInputStream来代替
        ByteArrayInputStream input = new ByteArrayInputStream(byteArray);

//        // 第四、反序列化
//        GeoDataOuterClass.GeoData person1 = GeoDataOuterClass.GeoData.parseFrom(input);
//        System.out.println("geometry:" + person1.getGeometry().toStringUtf8());

    }
}