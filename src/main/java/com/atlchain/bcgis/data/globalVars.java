package com.atlchain.bcgis.data;

import com.alibaba.fastjson.JSONArray;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

public class globalVars {
    public static List<Geometry> geometryList = new ArrayList<>();
    public static List<JSONArray> propList = new ArrayList<>();
}
