package flink.twitter.streaming.functions;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.PubNubException;
import com.pubnub.api.UserId;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.objects_api.channel.PNChannelMetadataResult;
import com.pubnub.api.models.consumer.objects_api.membership.PNMembershipResult;
import com.pubnub.api.models.consumer.objects_api.uuid.PNUUIDMetadataResult;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import com.pubnub.api.models.consumer.pubsub.PNSignalResult;
import com.pubnub.api.models.consumer.pubsub.files.PNFileEventResult;
import com.pubnub.api.models.consumer.pubsub.message_actions.PNMessageActionResult;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.utils.PubNubMessageParser;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Properties;

public class PubNubSource extends RichSourceFunction<Tweet> {

    private static final Logger LOG = Logger.getLogger(PubNubSource.class);
    private static final int MAX_RETRY = 3;
    private final Properties props;
    private volatile boolean isRunning = false;
    private volatile int retries = 0;
    private transient PubNub pubnub;

    public PubNubSource(Properties props) {
        this.props = props;
    }

    @Override
    public void run(SourceContext<Tweet> sourceContext) throws PubNubException, InterruptedException {

        // Config PubNub listener
        final UserId userId = new UserId(props.getProperty("user.id"));
        PNConfiguration pnConfiguration = new PNConfiguration(userId);
        pnConfiguration.setSubscribeKey(props.getProperty("subscribe.key"));

        pubnub = new PubNub(pnConfiguration);

        pubnub.addListener(new SubscribeCallback() {

            @Override
            public void status(PubNub pubnub, PNStatus status) {
                if (status.getCategory() == PNStatusCategory.PNConnectedCategory) {
                    // Just use the connected event to confirm you are subscribed for
                    // UI / internal notifications, etc
                    LOG.info("Connected PubNub successfully");
                } else if (status.getCategory() == PNStatusCategory.PNNetworkIssuesCategory ||
                        status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory
                ) {
                    // Handle connection issue
                    LOG.error("Connection error " + status.getCategory().name());
                    if (retries == MAX_RETRY){
                        LOG.error("Reaching max number of retries " + retries);
                        cancel();
                    } else {
                        LOG.info("Reconnect");
                        pubnub.reconnect();
                        retries++;
                    }
                }
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                // Process message received from PubNub
                try {
                    Tweet tweet = PubNubMessageParser.convertToTweet(message);
                    synchronized (sourceContext.getCheckpointLock()) {
                        sourceContext.collect(tweet);
                    }
                } catch (RuntimeException ex) {
                    // Something wrong happened due to format change
                    // Need to stop and investigate
                    LOG.error("Got exception with " + message.getMessage().toString(), ex);
                    cancel();
                    throw new RuntimeException("Got " + ex.getMessage() + " with " + message.getMessage().toString());
                }
            }

            @Override
            public void presence(@NotNull PubNub pubNub, @NotNull PNPresenceEventResult pnPresenceEventResult) {

            }

            @Override
            public void signal(PubNub pubnub, PNSignalResult pnSignalResult) {

            }

            @Override
            public void uuid(@NotNull PubNub pubNub, @NotNull PNUUIDMetadataResult pnuuidMetadataResult) {

            }

            @Override
            public void channel(@NotNull PubNub pubNub, @NotNull PNChannelMetadataResult pnChannelMetadataResult) {

            }

            @Override
            public void membership(@NotNull PubNub pubNub, @NotNull PNMembershipResult pnMembershipResult) {

            }

            @Override
            public void messageAction(@NotNull PubNub pubNub, @NotNull PNMessageActionResult pnMessageActionResult) {

            }

            @Override
            public void file(@NotNull PubNub pubNub, @NotNull PNFileEventResult pnFileEventResult) {

            }

        });

        // Start PubNub listener
        String channelName = props.getProperty("channel.name");
        pubnub.subscribe()
                .channels(Collections.singletonList(channelName))
                .execute();

        // Keep the thread running to do callback
        isRunning = true;
        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        LOG.info("Disconnecting PubNub");
        if (pubnub != null) {
            pubnub.forceDestroy();
        }
    }
}
