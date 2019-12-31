package com.atlchain.bcgis.data;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试 shpfile 插件对大数据的处理模式
 */
public class ShpDataStoreTest {
    private String shpURL = this.getClass().getResource("/D/D.shp").getFile();
    // 点    /beijing/P.shp
    // 线    /BL/BL.shp       /beijing/R.shp
    // 面    /D/D.shp        /Country_R/Country_R.shp    /Province/Province_R.shp   /chenduqu/chenduqu.shp
    private File shpFile = new File(shpURL);

    ShapefileDataStore shapeDataStore ;
    public ShpDataStoreTest() throws MalformedURLException {
        shapeDataStore = new ShapefileDataStore(shpFile.toURL());
    }


    /**
     * testGetTypeNames
     * @throws IOException
     */
    @Test
    public void testGetTypeNames() throws IOException {
        String[] typname = shapeDataStore.getTypeNames();
        System.out.println(typname[0]);
    }

    /**
     * testGetSchema
     * @throws Exception
     */
    @Test
    public void testGetSchema() throws Exception {

        SimpleFeatureType type = shapeDataStore.getSchema("D");

        String name = type.getTypeName();
        int count = type.getAttributeCount();
        System.out.println("feactureType count:" + count); // 得到属性的列数有多少
        System.out.println("feactureType attributes list:");
//        // 按列表访问     getAttributeDescriptors返回与指定名称匹配的属性描述符。
//        for (AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
//            System.out.println("  " + descriptor.getName());// 得到名称locations
//            System.out.println("(" + descriptor.getMinOccurs() + "," + descriptor.getMaxOccurs() + ",");
//            System.out.println((descriptor.isNillable() ? "nillable" : "manditory") + ")");
//            System.out.println("type:" + descriptor.getType().getBinding().getSimpleName());
//            System.out.println(" binding: " + descriptor.getType().getBinding().getSimpleName());
//        } // 返回的point和string 就是之前在column添加的元素
//
//
//
//        // 按索引访问
//        AttributeDescriptor attributeDescriptor = type.getDescriptor(0);
//        System.out.println("attribute 0    name: " + attributeDescriptor.getName());
//        System.out.println("attribute 0    type: " + attributeDescriptor.getType().toString());
//        System.out.println("attribute 0 binding: " + attributeDescriptor.getType().getBinding());
//
//        //按名字访问
//        AttributeDescriptor cityDescriptor = type.getDescriptor("CITY");
//        System.out.println("attribute 'CITY'    name: " + cityDescriptor.getName());
//        System.out.println("attribute 'CITT'    type: " + cityDescriptor.getType().toString());
//        System.out.println("attribute 'CITY' binding: " + cityDescriptor.getType().getBinding());
//
//        // 按地理坐标访问
//        GeometryDescriptor geometryDescriptor = type.getGeometryDescriptor();
//        System.out.println("default geom    name: " + geometryDescriptor.getName());
//        System.out.println("default geom    type: " + geometryDescriptor.getType().toString());
//        System.out.println("default geom binding: " + geometryDescriptor.getType().getBinding());
//        System.out.println("default geom     crs: " + CRS.toSRS(geometryDescriptor.getCoordinateReferenceSystem()));
//        System.out.println("example2 end\n");
    }

    @Test
    public void test() throws IOException {
        String name = shapeDataStore.getNames().get(0).toString();
        SimpleFeatureSource bcgisFeatureSource = shapeDataStore.getFeatureSource(name);
        SimpleFeatureCollection featureCollection = bcgisFeatureSource.getFeatures();
        ReferencedEnvelope env = DataUtilities.bounds(featureCollection);
        ReferencedEnvelope env1 = featureCollection.getBounds();
        System.out.println(env);
        System.out.println(env1);
    }


}
