package radish;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;

public class Config {

    private static Config INSTANCE;
    public final String accessToken;
    public final String accessTokenSecret;
    public final String apiKey;
    public final String apiSecretKey;
    public final Configuration hadoopConfiguration;
    private final boolean localMode;

    public Config(boolean localMode, String accessToken, String accessTokenSecret, String apiKey, String apiSecretKey) {
        this.localMode = localMode;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.apiKey = apiKey;
        this.apiSecretKey = apiSecretKey;
        this.hadoopConfiguration = new Configuration();

        if (localMode) {
            hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000");
        }
    }

    public Config(String accessToken, String accessTokenSecret, String apiKey, String apiSecretKey) {
        this(true, accessToken, accessTokenSecret, apiKey, apiSecretKey);
    }

    public static Config getInstance(boolean localMode) {
        if (Config.INSTANCE == null) {
            Dotenv dotenv;

            if (localMode) {
                dotenv = Dotenv.configure().load();
            } else {
                dotenv = Dotenv.configure().directory("/").load();
            }

            String accessToken = dotenv.get("ACCESS_TOKEN");
            String accessTokenSecret = dotenv.get("ACCESS_TOKEN_SECRET");
            String apiKey = dotenv.get("API_KEY");
            String apiSecretKey = dotenv.get("API_SECRET_KEY");

            Config.INSTANCE = new Config(localMode, accessToken, accessTokenSecret, apiKey, apiSecretKey);
        }

        return INSTANCE;
    }

    public static Config getInstance() {
        return getInstance(true);
    }
}
