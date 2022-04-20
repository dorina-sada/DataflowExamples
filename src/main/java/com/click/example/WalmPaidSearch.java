package com.click.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.sql.PreparedStatement;

public class WalmPaidSearch {
    public interface WalmPaidSearchOptions extends PipelineOptions {

    }

    static class WalmPaidSearchStatementSetter implements JdbcIO.PreparedStatementSetter<TableRow>
    {
        private static final long serialVersionUID = 1L;

        ////public interface WalmPaidSearchOptions extends PipelineOptions { }

        public void setParameters(TableRow element, PreparedStatement query) throws Exception
        {

            String file_id = (String) element.get("FileId");
            String geo = (String) element.get("GEO");
            String product = (String) element.get("Product");

            query.setLong(1, Long.valueOf(file_id));
            query.setLong(2, Long.valueOf(geo));
            query.setLong(3, Long.valueOf(product));

        }
    }

    public static void main(String[] args) {
        System.out.println(">>>>>>>>>>>>>>>>> " + args[0]);
        System.out.println(">>>>>>>>>>>>>>>>> " + args[1]);
        System.out.println(">>>>>>>>>>>>>>>>> " + args[2]);
        System.out.println(">>>>>>>>>>>>>>>>> " + args[3]);

        // PipelineOptionsFactory.register(BikeTripOptions.class);
        WalmPaidSearch.WalmPaidSearchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WalmPaidSearch.WalmPaidSearchOptions.class);
        Pipeline p = Pipeline.create(options);
        //jdbc:mysql:///<DATABASE_NAME>?cloudSqlInstance=<INSTANCE_CONNECTION_NAME>&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=<MYSQL_USER_NAME>&password=<MYSQL_USER_PASSWORD>
        // java.lang.IllegalArgumentException: Table reference is not in [project_id]:[dataset_id].[table_id] format: mmm-mmm-qa-mer-ff04.meridian_walmart_sds.Paid search
        //java.lang.IllegalArgumentException: Table reference is not in [project_id]:[dataset_id].[table_id] format: mmm-mmm-qa-mer-ff04.meridian_walmart_sds.Paid search
        // "bigquery-public-data:austin_bikeshare.bikeshare_trips"     "mmm-mmm-qa-mer-ff04:meridian_walmart_sds.Paid search"
        p
                .apply(BigQueryIO.read().from("bigquery-public-data:austin_bikeshare.bikeshare_trips"))
                .apply(JdbcIO.<TableRow>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create
                                        ("com.mysql.jdbc.Driver", "jdbc:postgresql://google/meridian_client_wmt?cloudSqlInstance=mmm-mmm-qa-mer-ff04:us-central1:mspostgresqqa-gcp-new&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=password&useSSL=false")
                                //  ("com.mysql.jdbc.Driver", "jdbc:mysql:///bq-to-cloudsql-test?cloudSqlInstance=sada-dorina:us-central1:bq-to-cloudsql-test&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=password&useSSL=false")
                        )
                        .withStatement("insert into source_datasets_wmt_test.PaidSearchFromBQ values(?,?,?)")
                        .withPreparedStatementSetter(new BikeTrip.BikeTripStatementSetter()));
        p.run().waitUntilFinish();
    }
}