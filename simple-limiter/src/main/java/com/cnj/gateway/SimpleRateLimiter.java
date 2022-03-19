package com.cnj.gateway;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.ratelimit.AbstractRateLimiter;
import org.springframework.cloud.gateway.route.RouteDefinitionRouteLocator;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.core.style.ToStringCreator;
import org.springframework.validation.annotation.Validated;
import reactor.core.publisher.Mono;

import javax.validation.constraints.Min;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cnj
 * @since 2022/3/19 下午3:44
 */
@Slf4j
public class SimpleRateLimiter extends AbstractRateLimiter<SimpleRateLimiter.Config> {

    public SimpleRateLimiter(ConfigurationService configurationService) {
        super(SimpleRateLimiter.Config.class, CONFIGURATION_PROPERTY_NAME, configurationService);
    }

    /**
     * This creates an instance with default static configuration, useful in Java DSL.
     *
     * @param defaultReplenishRate how many tokens per second in token-bucket algorithm.
     * @param defaultBurstCapacity how many tokens the bucket can hold in token-bucket
     *                             algorithm.
     */
    public SimpleRateLimiter(int defaultReplenishRate, int defaultBurstCapacity) {
        super(SimpleRateLimiter.Config.class, CONFIGURATION_PROPERTY_NAME, null);
        this.defaultConfig = new SimpleRateLimiter.Config().setReplenishRate(defaultReplenishRate)
                .setBurstCapacity(defaultBurstCapacity);
    }

    /**
     * This creates an instance with default static configuration, useful in Java DSL.
     *
     * @param defaultReplenishRate   how many tokens per second in token-bucket algorithm.
     * @param defaultBurstCapacity   how many tokens the bucket can hold in token-bucket
     *                               algorithm.
     * @param defaultRequestedTokens how many tokens are requested per request.
     */
    public SimpleRateLimiter(int defaultReplenishRate, int defaultBurstCapacity,
                             int defaultRequestedTokens) {
        this(defaultReplenishRate, defaultBurstCapacity);
        this.defaultConfig.setRequestedTokens(defaultRequestedTokens);
    }

    private Config defaultConfig;


    /**
     * In Memory Rate Limiter property name.
     */
    public static final String CONFIGURATION_PROPERTY_NAME = "in-memory-rate-limiter";

    /**
     * Remaining Rate Limit header name.
     */
    public static final String REMAINING_HEADER = "X-RateLimit-Remaining";

    /**
     * Replenish Rate Limit header name.
     */
    public static final String REPLENISH_RATE_HEADER = "X-RateLimit-Replenish-Rate";

    /**
     * Burst Capacity header name.
     */
    public static final String BURST_CAPACITY_HEADER = "X-RateLimit-Burst-Capacity";

    /**
     * Requested Tokens header name.
     */
    public static final String REQUESTED_TOKENS_HEADER = "X-RateLimit-Requested-Tokens";
    // configuration properties
    /**
     * Whether or not to include headers containing rate limiter information, defaults to
     * true.
     */
    private final boolean includeHeaders = true;

    /**
     * The name of the header that returns number of remaining requests during the current
     * second.
     */
    private final String remainingHeader = REMAINING_HEADER;

    /**
     * The name of the header that returns the replenish rate configuration.
     */
    private final String replenishRateHeader = REPLENISH_RATE_HEADER;

    /**
     * The name of the header that returns the burst capacity configuration.
     */
    private final String burstCapacityHeader = BURST_CAPACITY_HEADER;

    /**
     * The name of the header that returns the requested tokens configuration.
     */
    private final String requestedTokensHeader = REQUESTED_TOKENS_HEADER;

    protected SimpleRateLimiter(Class<Config> configClass, String configurationPropertyName, ConfigurationService configurationService) {
        super(configClass, configurationPropertyName, configurationService);
    }

    private final Map<String, RateLimiterTokenInfo> tokenBucket = new ConcurrentHashMap<>();

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        Config routeConfig = this.loadConfiguration(routeId);
        return Mono.fromSupplier(() -> {
            RateLimiterTokenInfo token = tokenBucket.compute(id, (key, value) -> {
                long now = Instant.now().getEpochSecond();
                RateLimiterTokenInfo tokenInfo = Objects.isNull(value) ? new RateLimiterTokenInfo(routeConfig.getBurstCapacity(), 0) : value;
                long lastTokens = tokenInfo.tokens;
                long lastRefreshed = tokenInfo.timestamp;
                long delta = Math.max(0, now - lastRefreshed);
                long fillTokens = Math.min(routeConfig.getBurstCapacity(), lastTokens + (delta * routeConfig.getReplenishRate()));
                long newTokens = fillTokens;
                boolean allowed = newTokens >= routeConfig.getRequestedTokens();
                if (allowed) {
                    newTokens = fillTokens - routeConfig.getRequestedTokens();
                }
                tokenInfo.setTokens(newTokens);
                tokenInfo.setTimestamp(now);
                tokenInfo.setAllowed(allowed);
                return tokenInfo;
            });
            boolean allowed = token.isAllowed();
            Long tokensLeft = token.tokens;
            Response response = new Response(allowed,
                    getHeaders(routeConfig, tokensLeft));
            if (log.isDebugEnabled()) {
                log.debug("response: " + response);
            }
            return response;
        });
    }

    public boolean isIncludeHeaders() {
        return includeHeaders;
    }

    public Map<String, String> getHeaders(Config config, Long tokensLeft) {
        Map<String, String> headers = new HashMap<>();
        if (isIncludeHeaders()) {
            headers.put(this.remainingHeader, tokensLeft.toString());
            headers.put(this.replenishRateHeader,
                    String.valueOf(config.getReplenishRate()));
            headers.put(this.burstCapacityHeader,
                    String.valueOf(config.getBurstCapacity()));
            headers.put(this.requestedTokensHeader,
                    String.valueOf(config.getRequestedTokens()));
        }
        return headers;
    }

    static class RateLimiterTokenInfo {

        public RateLimiterTokenInfo(int tokens, long timestamp) {
            this.tokens = tokens;
            this.timestamp = timestamp;
            this.allowed = true;
        }

        private boolean allowed;

        private long timestamp;

        private long tokens;

        public boolean isAllowed() {
            return allowed;
        }

        public void setAllowed(boolean allowed) {
            this.allowed = allowed;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getTokens() {
            return tokens;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setTokens(long tokens) {
            this.tokens = tokens;
        }

        @Override
        public String toString() {
            return "RateLimiterTokenInfo{" +
                    "timestamp=" + timestamp +
                    ", tokens=" + tokens +
                    '}';
        }
    }

    Config loadConfiguration(String routeId) {
        Config routeConfig = getConfig().getOrDefault(routeId, defaultConfig);

        if (routeConfig == null) {
            routeConfig = getConfig().get(RouteDefinitionRouteLocator.DEFAULT_FILTERS);
        }

        if (routeConfig == null) {
            throw new IllegalArgumentException(
                    "No Configuration found for route " + routeId + " or defaultFilters");
        }
        return routeConfig;
    }

    @Validated
    public static class Config {

        @Min(1)
        private int replenishRate;

        @Min(0)
        private int burstCapacity = 1;

        @Min(1)
        private int requestedTokens = 1;

        public int getReplenishRate() {
            return replenishRate;
        }

        public SimpleRateLimiter.Config setReplenishRate(int replenishRate) {
            this.replenishRate = replenishRate;
            return this;
        }

        public int getBurstCapacity() {
            return burstCapacity;
        }

        public SimpleRateLimiter.Config setBurstCapacity(int burstCapacity) {
            this.burstCapacity = burstCapacity;
            return this;
        }

        public int getRequestedTokens() {
            return requestedTokens;
        }

        public SimpleRateLimiter.Config setRequestedTokens(int requestedTokens) {
            this.requestedTokens = requestedTokens;
            return this;
        }

        @Override
        public String toString() {
            return new ToStringCreator(this).append("replenishRate", replenishRate)
                    .append("burstCapacity", burstCapacity)
                    .append("requestedTokens", requestedTokens).toString();

        }

    }
}
