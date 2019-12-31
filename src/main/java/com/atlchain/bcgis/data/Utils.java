package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atlchain.bcgis.data.protoBuf.protoConvert;
import org.glassfish.json.JsonUtil;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.io.*;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * 工具类
 */
public class Utils {

    /**
     * 构造GeometryCollection对象
     * @param geomList 空间几何对象列表
     * @return
     */
    public static GeometryCollection getGeometryCollection(Geometry[] geomList) {
        GeometryFactory geometryFactory = new GeometryFactory();
        return new GeometryCollection(geomList, geometryFactory);
    }

    public static byte[] getBytesFromGeometry(Geometry geometry) {
        WKBWriter writer = new WKBWriter();
        byte[] bytes = writer.write(geometry);
        return bytes;
    }

    public static Geometry getGeometryFromBytes(byte[] bytes) throws ParseException {
        Geometry geometry = new WKBReader().read(bytes);
        return geometry;
    }

    public static UUID getUuid() {
        UUID uuid = UUID.randomUUID();
        return uuid;
    }

    public static String getGeometryStr(ArrayList<Geometry> geometryArrayList) {
        StringBuilder builder = new StringBuilder();
        for (Geometry geometry : geometryArrayList) {
            builder.append(geometry.toString());
        }
        return builder.toString();
    }

    public static String getSHA256(String str) {
        if (str == null) {
            return null;}
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.reset();
            messageDigest.update(str.getBytes());
            return byte2Hex(messageDigest.digest());
        } catch (Exception e) {
            throw new RuntimeException(e);}
    }

    // StringBuffer 建立的字符串可以进行修改，并且不产生新的未使用对象
    private static String byte2Hex(byte[] bytes) {
        StringBuffer stringBuffer = new StringBuffer();
        String temp = null;
        for (int i = 0; i < bytes.length; i++) {
            temp = Integer.toHexString(bytes[i] & 0xFF);
            if (temp.length() == 1) {
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }

    // 多线程方式测试区块链读取数据的稳定性
    public static class ThreadDemo extends Thread {
        private File networkFile = new File(this.getClass().getResource("/network-config-test.yaml").toURI());
        public ThreadDemo(String string) throws URISyntaxException {
            super(string);
            System.out.println("====" + string);
        }

        @Override
        public void run() {
            Geometry geometry = null;
            int count = 1;
            synchronized (this) {
                BlockChainClient client = new BlockChainClient(networkFile);
                while (count > 0) {
                    String key = "Line4";
                    byte[][] result = client.getRecordBytes(
                            key,
                            "bcgiscc",
                            "GetRecordByKey"
                    );
                    try {
                        geometry = Utils.getGeometryFromBytes(result[0]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
//                    try {
//                        Thread.sleep(80);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    System.out.println("===============" + count );
                    count--;
                }
            }

        }
    }

    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);

            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 合并字节数组
     * @param bt1
     * @param bt2
     * @param count
     * @return
     */
    public static byte[][] byteMerger(byte[][] bt1, byte[][] bt2, int count){
        byte[][] bt3 = null;
        if(count == 0){
            bt3 = bt2;
        }else{
            bt3 = new byte[bt1.length+bt2.length][];
            System.arraycopy(bt1, 0, bt3, 0, bt1.length);
            System.arraycopy(bt2, 0, bt3, bt1.length, bt2.length);
        }
        return bt3;
    }

    /**
     * 将字节数组分割
     * @param bytes
     * @param size
     * @return
     */
    public static byte[] byteSpilt(byte[] bytes, int size){
        double splitLength = Double.parseDouble(size + "");
        int arrayLength = (int) Math.ceil(bytes.length / splitLength);

        byte[][] result = new byte[arrayLength][];
        int from, to;
        for (int i = 0; i < arrayLength; i++) {

            from = (int) (i * splitLength);
            to = (int) (from + splitLength);
            if (to > bytes.length)
                to = bytes.length;
            result[i] = Arrays.copyOfRange(bytes, from, to);
        }
        return bytes;
    }

    public static void main(String[] args) {
        byte[] bytes = "tefdsfsdfsadfsdfst".getBytes();
        byte[] bytes1 = byteSpilt(bytes, 3);
        System.out.println(bytes.length == bytes1.length);
    }

    /**
     * 将二维数组保存为本地文件
     */
    public static void saveByteToLocalFile(byte[][] results, String localFilePath, String fileName, String count){

        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file = null;
        for (byte[] resultByte : results) {
            try {
                File dir = new File(localFilePath);
                if(!dir.exists() && !dir.isDirectory()){//判断文件目录是否存在
                    dir.mkdirs();
                }
                file = new File(localFilePath + File.separator + fileName + count);
                fos = new FileOutputStream(file);
                bos = new BufferedOutputStream(fos);
                bos.write(resultByte);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            bos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件为 byte[]
     * @param file
     * @return
     */
    public static byte[] getFileBytes(String file) {
        try {
            File f = new File(file);
            int length = (int) f.length();
            byte[] data = new byte[length];
            FileInputStream in = new FileInputStream(f);
            in.read(data);
            in.close();
            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 将一维数组转为二维数组
     * @param onedouble
     * @return
     */
    public static byte[][] TwoArry(byte[] onedouble){
        byte[][] arr = new byte[1][onedouble.length];
        for (int i = 0; i < onedouble.length; i++) {
            arr[0][i] = onedouble[i];
        }
        return arr;
    }


}


















