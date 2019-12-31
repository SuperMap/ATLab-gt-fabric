package com.atlchain.bcgis.data.index;

import com.atlchain.bcgis.data.Shp2Wkb;
import com.atlchain.bcgis.data.Utils;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RTreeIndexTest {
    String indexFilePath = "E:\\SuperMapData\\RtreeData";
    private String shpURL = this.getClass().getResource("/D/D.shp").getFile();
    // 点    /beijing/P.shp
    // 线    /BL/BL.shp       /beijing/R.shp
    // 面    /D/D.shp        /Country_R/Country_R.shp    /Province/Province_R.shp   /chenduqu/chenduqu.shp
    private File shpFile = new File(shpURL);
    private Shp2Wkb shp2WKB = new Shp2Wkb(shpFile);
    List<Geometry> geometryList = shp2WKB.getGeometry();
    private RTreeIndex rTreeIndex = new RTreeIndex();

    public RTreeIndexTest() throws IOException {
    }

    @Test
    public void createRtree() {
        STRtree stRtree = rTreeIndex.createRtree(geometryList);
        System.out.println(stRtree.size());
        assertNotEquals(0, stRtree.size());
    }

    @Test
    public void query() {
        STRtree stRtree = rTreeIndex.createRtree(geometryList);
        Geometry searchGeo = rTreeIndex.generateSearchGeo(11401742  , 3530190, 11401742.1, 3530190.4);
//        System.out.println(searchGeo);
        List<Geometry> list = rTreeIndex.query(stRtree, searchGeo);
        for (Geometry geometry: list) {
            System.out.println(geometry);
        }
    }

    @Test
    public void saveSTRtree() {
        STRtree stRtree = rTreeIndex.createRtree(geometryList);
        // 新增保存路径
        String geometryStr = Utils.getGeometryStr((ArrayList<Geometry>) geometryList);
        String hash = Utils.getSHA256(geometryStr);
        indexFilePath = indexFilePath + File.separator + hash;
        rTreeIndex.saveSTRtree(stRtree, indexFilePath);
    }

    // 载入本地 R 树进行空间查询
    @Test
    public void loadSTRtree() {
        STRtree stRtree = rTreeIndex.loadSTRtree(indexFilePath);
        int depth = stRtree.depth();
        int expected = rTreeIndex.createRtree(geometryList).depth();
        assertEquals(expected, depth);

        Geometry searchGeo = rTreeIndex.generateSearchGeo(9215492, 5389752, 9215492.2, 5389752.2);
        List<Geometry> list = rTreeIndex.query(stRtree, searchGeo);
        for (Geometry geometry: list) {
            System.out.println(geometry);
        }
    }
}
