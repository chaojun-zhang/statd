package io.statd.server.config;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "cache")
@Data
@EnableCaching
public class CacheConfig extends CachingConfigurerSupport {
    private Map<String, String> cacheSpecs;


    @Bean
    public CacheManager cacheManager() {
        return new FlexibleCaffeineCacheManager(cacheSpecs);
    }

    private static class FlexibleCaffeineCacheManager extends CaffeineCacheManager {
        private final Map<String, Caffeine<Object, Object>> builders = new HashMap<>();

        private final CacheLoader cacheLoader;

        FlexibleCaffeineCacheManager(Map<String, String> cacheSpecs) {
            if (cacheSpecs != null) {
                for (Map.Entry<String, String> cacheSpecEntry : cacheSpecs.entrySet()) {
                    builders.put(cacheSpecEntry.getKey(), Caffeine.from(cacheSpecEntry.getValue()));
                }
            }
            this.cacheLoader = newCacheLoader();
        }

        private CacheLoader<Object, Object> newCacheLoader() {
            return new CacheLoader<Object, Object>() {
                @Override
                public Object load(Object key) throws Exception {
                    return null;
                }

                // Rewriting this method returns the oldValue value back to refresh the cache
                @Override
                public Object reload(Object key, Object oldValue) throws Exception {
                    return load(key);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        protected com.github.benmanes.caffeine.cache.Cache<Object, Object> createNativeCaffeineCache(String name) {
            Caffeine<Object, Object> builder = builders.computeIfAbsent(name, k -> Caffeine.newBuilder());
            return builder.build(cacheLoader);

        }
    }

    @Bean
    public KeyGenerator keyGenerator() {
        return new WorkingKeyGenerator();
    }

    static final class WorkingKeyGenerator implements KeyGenerator {

        @Override
        public Object generate(Object target, Method method, Object... params) {
            return new WorkingKey(target.getClass(), method.getName(), params);
        }

        /**
         * Like {@link org.springframework.cache.interceptor.SimpleKey} but considers the method.
         */
        final class WorkingKey {

            private final Class<?> clazz;
            private final String methodName;
            private final Object[] params;
            private final int hashCode;


            /**
             * Initialize a key.
             *
             * @param clazz      the receiver class
             * @param methodName the method name
             * @param params     the method parameters
             */
            WorkingKey(Class<?> clazz, String methodName, Object[] params) {
                this.clazz = clazz;
                this.methodName = methodName;
                this.params = params;
                int code = Arrays.deepHashCode(params);
                code = 31 * code + clazz.hashCode();
                code = 31 * code + methodName.hashCode();
                this.hashCode = code;
            }

            @Override
            public int hashCode() {
                return this.hashCode;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (!(obj instanceof WorkingKey)) {
                    return false;
                }
                WorkingKey other = (WorkingKey) obj;
                if (this.hashCode != other.hashCode) {
                    return false;
                }

                return this.clazz.equals(other.clazz)
                        && this.methodName.equals(other.methodName)
                        && Arrays.deepEquals(this.params, other.params);
            }

        }

    }

}

