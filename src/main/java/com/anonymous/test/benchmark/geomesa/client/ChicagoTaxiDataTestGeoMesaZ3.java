package com.anonymous.test.benchmark.geomesa.client;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author anonymous
 * @create 2022-01-26 12:52 PM
 **/
public class ChicagoTaxiDataTestGeoMesaZ3 implements CommonData {

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;

    private String dataFilename;
    private String queryFilename;

    public ChicagoTaxiDataTestGeoMesaZ3(String queryFilename, String dataFilename) {
        this.dataFilename = dataFilename;
        this.queryFilename = queryFilename;
    }

    @Override
    public String getTypeName() {
        return "chicago-taxi-data-geomesa";
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
          StringBuilder attributes = new StringBuilder();
          attributes.append("trip_id:String,");
          attributes.append("taxi_id:String,");
          attributes.append("dtg:Date,");
          attributes.append("trip_end_timestamp:Date,");
          attributes.append("trip_second:String,");
          attributes.append("trip_miles:String,");
          attributes.append("pickup_census_tract:String,");
          attributes.append("dropoff_census_tract:String,");
          attributes.append("pickup_community_area:String,");
          attributes.append("dropoff_community_area:String,");
          attributes.append("fare:String,");
          attributes.append("tips:String,");
          attributes.append("tolls:String,");
          attributes.append("extras:String,");
          attributes.append("trip_total:String,");
          attributes.append("payment_type:String,");
          attributes.append("company:String,");
          attributes.append("pickup_centroid_latitude:String,");
          attributes.append("pickup_centroid_longitude:String,");
          attributes.append("*geom:Point:srid=4326,");
          attributes.append("dropoff_centroid_latitude:String,");
          attributes.append("dropoff_centroid_longitude:String,");
          attributes.append("dropoff_centroid_location:String");

          sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());
          sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
          sft.getUserData().put("geomesa.indices.enabled", "z3");
          sft.getUserData().put("geomesa.z3.interval", "day");
        }

        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        // not used
        return features;
    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {
            try {
                List<Query> queries = new ArrayList<>();

                int count = 0;
                try {
                    BufferedReader in = new BufferedReader(new FileReader(queryFilename));
                    String str;
                    while ((str = in.readLine()) != null) {
                        String[] items = str.split(",");
                        String timeMin = items[1];
                        String timeMax = items[2];

                        String lonMin = items[3];
                        String lonMax = items[4];
                        String latMin = items[5];
                        String latMax = items[6];

                        String[] dateMinItems = timeMin.split(" ");
                        String[] dateMaxItems = timeMax.split(" ");
                        String during = String.format("dtg DURING %sT%s.000Z/%sT%s.000z", dateMinItems[0], dateMinItems[1], dateMaxItems[0], dateMaxItems[1]);
                        String bbox = String.format("bbox(geom,%s, %s, %s,%s)", lonMin, latMin, lonMax,  latMax);
                        System.out.println(during);
                        System.out.println(bbox);
                        Query query = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));

                        count++;

                        queries.add(query);

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }



                this.queries = Collections.unmodifiableList(queries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return queries;
    }


    @Override
    public void writeTestData(GeoMesaClient client, DataStore datastore, SimpleFeatureType sft) {
        // total record of chicago 198792904
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the CSV
            File input = new File(dataFilename);
            if (input == null) {
                throw new RuntimeException("Couldn't load resource csv");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            int count = 0;
            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                long start = System.currentTimeMillis();
                for (CSVRecord record : parser) {
                    //System.out.println(record.toString());
                    /*if ("".equals(record.get(17)) || "".equals(record.get(18)) || "".equals(record.get(20)) || "".equals(record.get(21))) {
                        System.out.println("bingo");
                        continue;
                    }*/
                    if ("".equals(record.get(18)) || "".equals(record.get(17)) || "".equals(record.get(19)) || "".equals(record.get(2)) || "".equals(record.get(3))) {
                        continue;
                    }

                    // pull out the fields corresponding to our simple feature attributes
                    builder.set("trip_id", record.get(0));
                    builder.set("taxi_id", record.get(1));
                    builder.set("dtg", Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)));
                    builder.set("trip_end_timestamp", Date.from(LocalDateTime.parse(record.get(3), dateFormat).toInstant(ZoneOffset.UTC)));

                    builder.set("trip_second", record.get(4));
                    builder.set("trip_miles", record.get(5));
                    builder.set("pickup_census_tract", record.get(6));
                    builder.set("dropoff_census_tract", record.get(7));
                    builder.set("pickup_community_area", record.get(8));
                    builder.set("dropoff_community_area", record.get(9));
                    builder.set("fare", record.get(10));
                    builder.set("tips", record.get(11));
                    builder.set("tolls", record.get(12));
                    builder.set("extras", record.get(13));
                    builder.set("trip_total", record.get(14));
                    builder.set("payment_type", record.get(15));
                    builder.set("company", record.get(16));
                    builder.set("geom", record.get(17));
                    builder.set("pickup_centroid_longitude", record.get(18));


                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering

                    double longitude = Double.parseDouble(record.get(18));
                    double latitude = Double.parseDouble(record.get(17));
                    builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                    builder.set("dropoff_centroid_latitude", record.get(21));
                    builder.set("dropoff_centroid_longitude", record.get(20));
                    /*double dropoffLongitude = Double.parseDouble(record.get(21));
                    double dropoffLatitude = Double.parseDouble(record.get(20));
                    builder.set("dropoff_centroid_location", "POINT (" + dropoffLongitude + " " + dropoffLatitude + ")");*/
                    builder.set("dropoff_centroid_location", record.get(22));


                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(record.get(0)));
                    count++;
                    if (count % 8192 == 0) {
                        client.writeFeatures(datastore, sft, Collections.unmodifiableList(features));
                        features.clear();
                    }
                    if (count % 1000000 == 0) {
                        System.out.println("Finish count: " + count);
                        System.out.println("Take time: " + ((System.currentTimeMillis() - start) / 1000.0) + " s");
                    }

                }
                client.writeFeatures(datastore, sft, Collections.unmodifiableList(features));
                features.clear();
                System.out.println("Write total records: " + count);
                System.out.println("Total time: " + ((System.currentTimeMillis() - start) / 1000.0) + " s");
            } catch (IOException e) {
                throw new RuntimeException("Error reading taxi data:", e);
            }

        }
    }

    @Override
    public Filter getSubsetFilter() {
        return Filter.INCLUDE;
    }
}
