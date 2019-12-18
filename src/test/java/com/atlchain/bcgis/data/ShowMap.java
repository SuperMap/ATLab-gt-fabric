package com.atlchain.bcgis.data;

import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.geotools.swing.JMapFrame;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.IOException;

public class ShowMap {
    /**
     * 以 Frame 方式显示地图
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String DKey = "6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4";
        BCGISDataStore bcgisDataStore = new BCGISDataStore(
                new File(BCGISDataStoreTest.class.getResource("/network-config-test.yaml").getPath()),
                new File("E:\\SuperMapData\\D\\D.shp"),
                "bcgiscc",
                "GetRecordByKey",
                DKey
        );
        SimpleFeatureSource simpleFeatureSource = bcgisDataStore.getFeatureSource(bcgisDataStore.getTypeNames()[0]);
        simpleFeatureSource.getSchema();
        String typeName = bcgisDataStore.getTypeNames()[0];
        SimpleFeatureType type = bcgisDataStore.getSchema(typeName);

        MapContent map = new MapContent();
        map.setTitle("testBCGIS");
//        Style style = SLD.createLineStyle(Color.BLACK, 2.0f);
        Style style = SLD.createSimpleStyle(type);

        Layer layer = new FeatureLayer(simpleFeatureSource, style);
        map.addLayer(layer);
        JMapFrame.showMap(map);
    }
}
