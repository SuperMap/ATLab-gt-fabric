/**
 * 利用R树构造空间索引。
 */

package com.atlchain.bcgis.data.index;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RTreeIndex {
    private STRtree stRtree = new STRtree();

    /**
     * 建立R树索引
     * @return
     */
    public STRtree createRtree(List<Geometry> geometryList){
        for (Geometry geometry: geometryList) {
            Envelope envelope = geometry.getEnvelopeInternal();
            stRtree.insert(envelope, geometry);
        }
        stRtree.build();
        return stRtree;
    }
    /**
     * R树查询
     * @param stRtree
     * @param searchGeo
     * @return
     */
    public List<Geometry> query(STRtree stRtree, Geometry searchGeo){
        List<Geometry> result = new ArrayList<>();
        @SuppressWarnings("rawtypes")
        List list = stRtree.query(searchGeo.getEnvelopeInternal());
        for(int i = 0;i < list.size(); i++) {
            Geometry lineStr = (Geometry)list.get(i);
            if(lineStr.intersects(searchGeo)) {
                result.add(lineStr);
            }
        }
        return result;
    }

    //根据两点生成矩形搜索框
    public static Geometry generateSearchGeo(double left_top_x, double left_top_y, double right_bottom_x, double right_bottom_y){
        Coordinate[] coors = new Coordinate[5];
        coors[0] = new Coordinate(left_top_x, left_top_y);
        coors[1] = new Coordinate(right_bottom_x, left_top_y);
        coors[2] = new Coordinate(left_top_x, right_bottom_y);
        coors[3] = new Coordinate(right_bottom_x, right_bottom_y);
        coors[4] = new Coordinate(left_top_x, left_top_y);
        LinearRing ring = new LinearRing(new CoordinateArraySequence(coors), new GeometryFactory());
        return ring;
    }

    public void saveSTRtree(STRtree stRtree, String filePath) {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            fos = new FileOutputStream(new File(filePath));
            oos = new ObjectOutputStream(fos);
            oos.writeObject(stRtree);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public STRtree loadSTRtree(String filePath) {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        STRtree stRtree = null;
        try {
            fis = new FileInputStream(new File(filePath));
            ois = new ObjectInputStream(fis);
            stRtree =(STRtree)ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
                ois.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return stRtree;
    }

}
