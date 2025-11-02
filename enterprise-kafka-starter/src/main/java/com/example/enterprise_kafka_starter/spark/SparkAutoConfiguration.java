package com.example.enterprise_kafka_starter.spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(SparkSession.class)
public class SparkAutoConfiguration {
    //    @Value("${dl.account}")
        private String account = "storageaccountdemoapp";

    //    @Value("${dl.oauth.tenantId}")
    private String tenantId = "ee785629-a32d-455e-9974-d0eb9ac5814d";

    //    @Value("${dl.oauth.clientId}")
    private String clientId = "d15e7298-5aad-4bcf-ae8d-f7350e6e9c0b";

    //    @Value("${dl.oauth.clientSecret}")
    private String clientSecret = "qOM8Q~3jAqtUalH2Ih9bIMoswIdyVIvFpOLJhcw5s";

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean(SparkSession.class)
    public SparkSession spark() {
        return SparkSession.builder()
                .appName("demoApp")
                .master("local[*]") // POC; in prod prefer a real Spark cluster
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // ---- ABFS OAuth (Service Principal) ----
                .config("fs.azure.account.auth.type." + account + ".dfs.core.windows.net", "OAuth")
                .config("fs.azure.account.oauth.provider.type." + account + ".dfs.core.windows.net",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config("fs.azure.account.oauth2.client.id." + account + ".dfs.core.windows.net", clientId)
                .config("fs.azure.account.oauth2.client.secret." + account + ".dfs.core.windows.net", clientSecret)
                .config("fs.azure.account.oauth2.client.endpoint." + account + ".dfs.core.windows.net",
                        "https://login.microsoftonline.com/" + tenantId + "/oauth2/token")

                // JVM module flags (future-proof when Spark forks executors)
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions",
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")

                // disable Spark UI to avoid javax.servlet dependency
                .config("spark.ui.enabled", "false")
                .config("spark.ui.showConsoleProgress", "false")

                // (Optional) write tuning for small micro-batches
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.files.maxRecordsPerFile", "200000")
                .getOrCreate();
    }
}