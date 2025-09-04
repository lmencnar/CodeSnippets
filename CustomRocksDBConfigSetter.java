package com.globomantics;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.BlockBasedTableConfigWithAccessibleCache;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomRocksDBConfigSetter implements RocksDBConfigSetter {
    private static final Logger log = LoggerFactory.getLogger(CustomRocksDBConfigSetter.class);

    private static final int BLOCK_SIZE_MB = 64; // Block cache size in MB
    private static final int WRITE_BUFFER_SIZE_MB = 4; // Write buffer size in MB
    private static final int MAX_WRITE_BUFFER_NUMBER = 3;  // Max write buffer number
    // Use Options as key to ensure unique cache per RocksDB instance
    private static final Map<Options, Cache> cacheMap = new java.util.concurrent.ConcurrentHashMap<>();

    private static final AtomicInteger storeCount = new AtomicInteger(0);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        storeCount.incrementAndGet();

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfigWithAccessibleCache) options.tableFormatConfig();
        Cache cache = new LRUCache(BLOCK_SIZE_MB * 1024 * 1024L);
        // options is used as key to ensure unique cache per RocksDB instance
        // options is unique per store instance
        cacheMap.put(options, cache);
        tableConfig.setBlockCache(cache);
        options.setTableFormatConfig(tableConfig);

        options.setWriteBufferSize(WRITE_BUFFER_SIZE_MB * 1024 * 1024L); // 4 MB write buffer
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        log.info("RocksDB store [{}] configured with block cache: {} MB, write buffer: {} MB, storeCount={}", storeName,
                BLOCK_SIZE_MB, options.writeBufferSize() / (1024 * 1024), storeCount.get());
    }

    @Override
    public void close(String storeName, Options options) {
        storeCount.decrementAndGet();

        log.info("Closing RocksDB store [{}] storeCount={}", storeName, storeCount.get());

        Cache cache = cacheMap.remove(options);
        if (cache != null) {
            log.info("Closing cache for store [{}], storeCount={}, usage={}, pinnedUsage={}",
                    storeName, storeCount.get(), cache.getUsage(), cache.getPinnedUsage());
            cache.close();
        } else {
            log.warn("No cache found for store [{}] during close operation", storeName);
        }
    }
}
