package com.click.example;


import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import java.sql.PreparedStatement;

public class BikeTrip_postgresql {
//    public interface BikeTrip_postgresqlOptions extends PipelineOptions {
//        @Description("subnetwork")
//        String getSubnetwork();
//        void setSubnetwork(String value);
//    }

    public interface BikeTrip_postgresqlOptions extends PipelineOptions {

    }

    static class BikeTrip_postgresqlSetter implements JdbcIO.PreparedStatementSetter<TableRow>
    {
        private static final long serialVersionUID = 1L;

        //public interface BikeTrip_postgresqlOptions extends PipelineOptions { }

        public void setParameters(TableRow element, PreparedStatement query) throws Exception
        {

            String trip_id = (String) element.get("trip_id");
            String subscriber_type = (String) element.get("subscriber_type");
            String start_station_name = (String) element.get("start_station_name");
            String end_station_name = (String) element.get("end_station_name");

            query.setLong(1, Long.valueOf(trip_id));
            query.setString(2, subscriber_type);
            query.setString(3, start_station_name);
            query.setString(4, end_station_name);

        }
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(BikeTrip_postgresqlOptions.class);

        // PipelineOptionsFactory.register(BikeTripOptions.class);
        BikeTrip_postgresql.BikeTrip_postgresqlOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BikeTrip_postgresql.BikeTrip_postgresqlOptions.class);
        //options.setSubnetwork(args[4]);
        Pipeline p = Pipeline.create(options);
        //bigquery-public-data:austin_bikeshare.bikeshare_trips
        //String tableName = "Paid search";

        p
                //.apply(BigQueryIO.read().from("sada-dorina:dev_test.Paid search"))
                .apply(BigQueryIO.read().fromQuery("SELECT * FROM [sada-dorina:dev_test.Paid search]"))
                .apply(JdbcIO.<TableRow>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create
                                        ("org.postgresql.Driver", "jdbc:postgresql://google/bikeshare?cloudSqlInstance=sada-dorina:us-central1:postgress-instance-test&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=postgres&password=password&useSSL=false")

                        )
                        .withStatement("insert into schema_test.trips values(?,?,?,?)")
                        .withPreparedStatementSetter(new BikeTrip_postgresql.BikeTrip_postgresqlSetter()));
        p.run().waitUntilFinish();
    }
}
