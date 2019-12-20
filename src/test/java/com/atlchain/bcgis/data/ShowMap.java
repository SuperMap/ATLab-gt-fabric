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
        String DKey = "d7e94bf0c86c94579e8b564d2dea995ed3746108f98f003fb555bcd41831f885";
        // d7e94bf0c86c94579e8b564d2dea995ed3746108f98f003fb555bcd41831f885
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
