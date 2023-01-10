package com.anonymous.test.benchmark.geomesa.client;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author anonymous
 * @create 2022-10-04 8:26 PM
 **/
public class GeoMesaPortoTaxiData implements CommonData{

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;

    private String dataFilename;
    private String queryFilename;
    private String queryType;

    public GeoMesaPortoTaxiData(String dataFilename, String queryFilename, String queryType) {
        this.dataFilename = dataFilename;
        this.queryFilename = queryFilename;
        this.queryType = queryType;
    }

    @Override
    public String getTypeName() {
        return "porto-taxi-data";
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            StringBuilder attributes = new StringBuilder();
            attributes.append("unique_id:String,");
            attributes.append("taxi_id:String:index=true,"); // marks this attribute for indexing
            attributes.append("payload:String,");
            attributes.append("dtg:Date,");
            attributes.append("*geom:Point:srid=4326");


            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());
            sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
            sft.getUserData().put("geomesa.indices.enabled", "z3,attr:taxi_id:dtg");
            //sft.getUserData().put("geomesa.z3.interval", "day");
        }

        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        // not used
        return null;
    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {
            try {
                List<Query> queries = new ArrayList<>();

                if ("idtemporal".equals(queryType)) {
                    List<IdTemporalQueryPredicate> predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile(queryFilename);
                    for (IdTemporalQueryPredicate predicate : predicateList) {
                        Date dateMin = new Date(predicate.getStartTimestamp()-1000);
                        Date dateMax = new Date(predicate.getStopTimestamp()+1000);
                        String[] dateMinItems = fromDateToString(dateMin).split(" ");
                        String[] dateMaxItems = fromDateToString(dateMax).split(" ");
                        String during = String.format("dtg DURING %sT%s.000Z/%sT%s.000z", dateMinItems[0], dateMinItems[1], dateMaxItems[0], dateMaxItems[1]);

                        String attr = String.format("taxi_id = '%s'", predicate.getDeviceId());

                        Query query = new Query(getTypeName(), ECQL.toFilter(attr + " AND " + during));

                        queries.add(query);
                    }
                }
                if ("spatiotemporal".equals(queryType)) {
                    List<SpatialTemporalRangeQueryPredicate> predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile(queryFilename);
                    for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
                        Date dateMin = new Date(predicate.getStartTimestamp()-1000);
                        Date dateMax = new Date(predicate.getStopTimestamp()+1000); // add 1 s for inclusion boundary
                        String[] dateMinItems = fromDateToString(dateMin).split(" ");
                        String[] dateMaxItems = fromDateToString(dateMax).split(" ");

                        String during = String.format("dtg DURING %sT%s.000Z/%sT%s.000z", dateMinItems[0], dateMinItems[1], dateMaxItems[0], dateMaxItems[1]);

                        double lonMin = predicate.getLowerLeft().getLongitude();
                        double latMin = predicate.getLowerLeft().getLatitude();
                        double lonMax = predicate.getUpperRight().getLongitude();
                        double latMax = predicate.getUpperRight().getLatitude();
                        String bbox = String.format("bbox(geom,%f, %f, %f,%f)", lonMin, latMin, lonMax, latMax);
                        System.out.println(during);
                        System.out.println(bbox);
                        Query query = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));
                        //Query query = new Query(getTypeName(), ECQL.toFilter(bbox));


                        queries.add(query);
                    }
                }


                this.queries = Collections.unmodifiableList(queries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return queries;
    }

    @Override
    public Filter getSubsetFilter() {
        return Filter.INCLUDE;
    }

    @Override
    public void writeTestData(GeoMesaClient client, DataStore datastore, SimpleFeatureType sft) {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();
            PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFilename);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            int count = 0;
            TrajectoryPoint point;
            try {
                long start = System.currentTimeMillis();
                while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
                    // pull out the fields corresponding to our simple feature attributes
                    String unique_id = point.getOid() + "." + point.getTimestamp();
                    builder.set("unique_id", unique_id);
                    builder.set("taxi_id", point.getOid());
                    builder.set("payload", point.getPayload());
                    builder.set("dtg", new Date(point.getTimestamp()));
                    //System.out.println(new Date(point.getTimestamp()));

                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering
                    builder.set("geom", "POINT (" + point.getLongitude() + " " + point.getLatitude() + ")");


                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(unique_id));
                    count++;
                    if (count % 20000 == 0) {
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

    public static String fromDateToString(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone utc = TimeZone.getTimeZone("UTC");
        dateFormat.setTimeZone(utc);
        return dateFormat.format(date);
    }

    public static void main(String[] args) {
        /*DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

        Date date = Date.from(LocalDateTime.parse("2010-01-02 12:22:10", dateFormat).toInstant(ZoneOffset.UTC));*/
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone utc = TimeZone.getTimeZone("UTC");
        dateFormat.setTimeZone(utc);
        Date date1 = new Date(1372636853000L);
        System.out.println(dateFormat.format(date1));
        System.out.println(date1);
    }
}
