package com.cnj.gateway.limiter;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.ratelimit.AbstractRateLimiter;
import org.springframework.cloud.gateway.route.RouteDefinitionRouteLocator;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.validation.annotation.Validated;
import reactor.core.publisher.Mono;

import javax.validation.constraints.Min;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author cnj
 * @since 2022/3/19 下午5:29
 */
@Slf4j
public class GuavaRateLimiter extends AbstractRateLimiter<GuavaRateLimiter.Config> {


    public GuavaRateLimiter(ConfigurationService configurationService) {
        super(Config.class, CONFIGURATION_PROPERTY_NAME, configurationService);
    }

    /**
     * This creates an instance with default static configuration, useful in Java DSL.
     *
     * @param defaultReplenishRate how many tokens per second in token-bucket algorithm.
     * @param defaultTryAcquireTimeout
     */
    public GuavaRateLimiter(int defaultReplenishRate, int defaultTryAcquireTimeout) {
        super(Config.class, CONFIGURATION_PROPERTY_NAME, (ConfigurationService) null);
        this.defaultConfig = new Config().setReplenishRate(defaultReplenishRate)
                .setTryAcquireTimeout(defaultTryAcquireTimeout);
    }

    /**
     * This creates an instance with default static configuration, useful in Java DSL.
     *
     * @param defaultReplenishRate   how many tokens per second in token-bucket algorithm.
     * @param defaultBurstCapacity   how many tokens the bucket can hold in token-bucket
     *                               algorithm.
     * @param defaultRequestedTokens how many tokens are requested per request.
     */
    public GuavaRateLimiter(int defaultReplenishRate, int defaultBurstCapacity,
                            int defaultRequestedTokens) {
        this(defaultReplenishRate, defaultBurstCapacity);
        this.defaultConfig.setRequestedTokens(defaultRequestedTokens);
    }

    private Config defaultConfig;


    /**
     * In Memory Rate Limiter property name.
     */
    public static final String CONFIGURATION_PROPERTY_NAME = "guava-rate-limiter";

    /**
     * Remaining Rate Limit header name.
     */
    public static final String REMAINING_HEADER = "X-RateLimit-Remaining";

    /**
     * Replenish Rate Limit header name.
     */
    public static final String REPLENISH_RATE_HEADER = "X-RateLimit-Replenish-Rate";

    /**
     * Try Acquire Timeout.
     */
    public static final String TRY_ACQUIRE_TIMEOUT_HEADER = "X-RateLimit-Try-Acquire-Timeout";

    /**
     * Requested Tokens header name.
     */
    public static final String REQUESTED_TOKENS_HEADER = "X-RateLimit-Requested-Tokens";
    // configuration properties
    /**
     * Whether or not to include headers containing rate limiter information, defaults to
     * true.
     */
    private boolean includeHeaders = true;

    /**
     * The name of the header that returns number of remaining requests during the current
     * second.
     */
    private String remainingHeader = REMAINING_HEADER;

    /**
     * The name of the header that returns the replenish rate configuration.
     */
    private String replenishRateHeader = REPLENISH_RATE_HEADER;

    /**
     */
    private String tryAcquireTimeoutHeader = TRY_ACQUIRE_TIMEOUT_HEADER;

    /**
     * The name of the header that returns the requested tokens configuration.
     */
    private String requestedTokensHeader = REQUESTED_TOKENS_HEADER;

    protected GuavaRateLimiter(Class<Config> configClass, String configurationPropertyName, ConfigurationService configurationService) {
        super(configClass, configurationPropertyName, configurationService);
    }

    /**
     * 不同的方法存放不同的令牌桶
     */
    private final Map<String, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        Config routeConfig = this.loadConfiguration(routeId);
        return Mono.fromSupplier(() -> {
            RateLimiter rateLimiter = rateLimiterMap.computeIfAbsent(id, key -> RateLimiter.create(routeConfig.getReplenishRate()));
            boolean acquired = rateLimiter.tryAcquire(routeConfig.getRequestedTokens(), routeConfig.getTryAcquireTimeout(), TimeUnit.MILLISECONDS);
            Response response = new Response(acquired, getHeaders(routeConfig, 1L));
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
            headers.put(this.tryAcquireTimeoutHeader,
                    String.valueOf(config.getTryAcquireTimeout()));
            headers.put(this.requestedTokensHeader,
                    String.valueOf(config.getRequestedTokens()));
        }
        return headers;
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

        @Min(1)
        private int requestedTokens = 1;

        /**
         * 尝试获取令牌的时间
         */
        @Min(0)
        private int tryAcquireTimeout = 0;

        public int getReplenishRate() {
            return replenishRate;
        }

        public Config setReplenishRate(int replenishRate) {
            this.replenishRate = replenishRate;
            return this;
        }

        public int getRequestedTokens() {
            return requestedTokens;
        }

        public Config setRequestedTokens(int requestedTokens) {
            this.requestedTokens = requestedTokens;
            return this;
        }

        public int getTryAcquireTimeout() {
            return tryAcquireTimeout;
        }

        public Config setTryAcquireTimeout(int tryAcquireTimeout) {
            this.tryAcquireTimeout = tryAcquireTimeout;
            return this;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "replenishRate=" + replenishRate +
                    ", requestedTokens=" + requestedTokens +
                    ", tryAcquireTimeout=" + tryAcquireTimeout +
                    '}';
        }
    }

}
