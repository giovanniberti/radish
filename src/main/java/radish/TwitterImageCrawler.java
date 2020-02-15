package radish;

import io.github.cdimascio.dotenv.Dotenv;
import winterwell.jtwitter.OAuthSignpostClient;
import winterwell.jtwitter.Status;
import winterwell.jtwitter.Twitter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TwitterImageCrawler {
    private static final String ACCESS_TOKEN = Dotenv.load().get("ACCESS_TOKEN");
    private static final String ACCESS_TOKEN_SECRET = Dotenv.load().get("ACCESS_TOKEN_SECRET");
    private static final String API_KEY = Dotenv.load().get("API_KEY");
    private static final String API_SECRET_KEY = Dotenv.load().get("API_SECRET_KEY");

    private Twitter twitter;

    public TwitterImageCrawler() {
        OAuthSignpostClient client = new OAuthSignpostClient(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
        this.twitter = new Twitter("bertigiova", client);
        twitter.setIncludeTweetEntities(true);
    }

    public List<String> crawlKeyword(String keyword) {
        List<Status> statuses = twitter.search(keyword + " filter:images");
        List<String> mediaUrls = statuses.stream()
                .flatMap(s -> s.getTweetEntities(Twitter.KEntityType.media)
                        .stream()
                        .map(Twitter.TweetEntity::mediaUrl)
                        .filter(Objects::nonNull))
                .collect(Collectors.toList());

        return mediaUrls;
    }
}
