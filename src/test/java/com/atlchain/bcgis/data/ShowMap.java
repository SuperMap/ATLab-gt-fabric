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
        String Key = "8407bd3cd93d156e026b3cccba12035ef10b85b1ba1db31590296a153af7f3db";
        //   D               6bff876faa82c51aee79068a68d4a814af8c304a0876a08c0e8fe16e5645fde4       （OK）
        //  Province         23c5d6fc5e2794a264c72ae9e8e3281a7072696dc5f93697b8b5ef1e803fd3d8       (OK)
        //  BL              d7e94bf0c86c94579e8b564d2dea995ed3746108f98f003fb555bcd41831f885        (OK)
        //  P               b6a5833aba1f3a73e9d721a6df15defd00b17a3722491bb33b7700d37f288d5b
        // Country_R        8407bd3cd93d156e026b3cccba12035ef10b85b1ba1db31590296a153af7f3db          (OK)
        // beijing/R       278934ff40e23d4a054144b495df7ca5eb0f764aa02d44f0cf02b8921539d8b1
        // chenduqu         5668c664c852b2b95543b784371f0267136cb4e09b8cb4a284148d2b9f578301
        BCGISDataStore bcgisDataStore = new BCGISDataStore(
                new File(BCGISDataStoreTest.class.getResource("/network-config-test.yaml").getPath()),
                new File("E:\\SuperMapData\\D\\D.shp"),
                "bcgiscc",
                "GetRecordByKey",
                Key
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
