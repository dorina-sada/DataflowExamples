package com.click.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.sql.PreparedStatement;

public class BikeTrip {
    public interface BikeTripOptions extends PipelineOptions {

    }

    static class BikeTripStatementSetter implements JdbcIO.PreparedStatementSetter<TableRow>
    {
        private static final long serialVersionUID = 1L;

        public interface BikeTripOptions extends PipelineOptions { }

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

        // PipelineOptionsFactory.register(BikeTripOptions.class);
        BikeTripOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BikeTripOptions.class);
        Pipeline p = Pipeline.create(options);
        //jdbc:mysql:///<DATABASE_NAME>?cloudSqlInstance=<INSTANCE_CONNECTION_NAME>&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=<MYSQL_USER_NAME>&password=<MYSQL_USER_PASSWORD>

        p
                .apply(BigQueryIO.read().from("bigquery-public-data:austin_bikeshare.bikeshare_trips"))
                .apply(JdbcIO.<TableRow>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create
                                        ("com.mysql.jdbc.Driver", "jdbc:mysql://google/bikeshare?cloudSqlInstance=sada-dorina:us-central1:bq-to-cloudsql-test&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=password&useSSL=false")
                                //  ("com.mysql.jdbc.Driver", "jdbc:mysql:///bq-to-cloudsql-test?cloudSqlInstance=sada-dorina:us-central1:bq-to-cloudsql-test&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=password&useSSL=false")
                        )
                        .withStatement("insert into trips values(?,?,?,?)")
                        .withPreparedStatementSetter(new BikeTripStatementSetter()));
        p.run().waitUntilFinish();
    }

}
