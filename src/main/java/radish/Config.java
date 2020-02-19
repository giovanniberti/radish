package radish;

import io.github.cdimascio.dotenv.Dotenv;

public class Config {

    private static Config INSTANCE;
    public final String accessToken;
    public final String accessTokenSecret;
    public final String apiKey;
    public final String apiSecretKey;

    public Config(String accessToken, String accessTokenSecret, String apiKey, String apiSecretKey) {
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.apiKey = apiKey;
        this.apiSecretKey = apiSecretKey;
    }

    public static Config getInstance() {
        if (Config.INSTANCE == null) {
            Dotenv dotenv = Dotenv.configure().directory("/").load();
            String accessToken = dotenv.get("ACCESS_TOKEN");
            String accessTokenSecret = dotenv.get("ACCESS_TOKEN_SECRET");
            String apiKey = dotenv.get("API_KEY");
            String apiSecretKey = dotenv.get("API_SECRET_KEY");

            Config.INSTANCE = new Config(accessToken, accessTokenSecret, apiKey, apiSecretKey);
        }

        return INSTANCE;
    }
}
