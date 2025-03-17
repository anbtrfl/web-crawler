package info.kgeorgiy.ja.riazanova.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;


public class WebCrawler implements NewCrawler {
    private final Downloader downloader;
    private final ExecutorService downloadingExecutor;
    private final ExecutorService extractingExecutor;
    private final int perHost;

    /**
     * thread-safe WebCrawler class that crawls sites recursively
     *
     * @param downloader  allows you to download pages and extract links from them.
     * @param downloaders maximum number of simultaneously loaded pages.
     * @param extractors  the maximum number of pages from which links are retrieved simultaneously.
     * @param perHost     the maximum number of pages downloaded simultaneously from one host. To determine the host, you should use the getHost method of the URLUtils class from the tests.
     */
    public WebCrawler(Downloader downloader,
                      int downloaders,
                      int extractors,
                      int perHost) {
        this.downloader = downloader;
        this.downloadingExecutor = Executors.newFixedThreadPool(downloaders);
        this.extractingExecutor = Executors.newFixedThreadPool(extractors);
        this.perHost = perHost;
    }

    /**
     * Closes this crawler, freeing any allocated resources.
     */
    @Override
    public void close() {
        downloadingExecutor.shutdown();
        extractingExecutor.shutdown();
    }

    @Override
    public Result download(String url, int depth, Set<String> excludes) {
        Map<String, IOException> errors = new ConcurrentHashMap<>();
        Set<String> downloaded = ConcurrentHashMap.newKeySet();
        Set<String> extracted = ConcurrentHashMap.newKeySet();
        Set<String> visited = ConcurrentHashMap.newKeySet();
        Phaser phaser = new Phaser(1);

        Set<String> results = ConcurrentHashMap.newKeySet();
        results.add(url);
        visited.add(url);
        for (int i = 0; i < depth; i++) {
            if (i != 0) {
                results.clear();
                results.addAll(extracted);
                extracted.clear();
            }
            for (String res : results) {
                phaser.register();
                Runnable r = newDownload(res, depth - i, excludes, errors, downloaded, extracted, visited, phaser);
                if (r != null) {
                    downloadingExecutor.submit(r);
                }
            }
            phaser.arriveAndAwaitAdvance();
        }
        return new Result(new ArrayList<>(downloaded), errors);
    }

    private Runnable newDownload(final String url,
                                 final int depth,
                                 Set<String> excludes,
                                 Map<String, IOException> errors,
                                 Set<String> results,
                                 Set<String> extracted,
                                 Set<String> visited,
                                 Phaser phaser) {
        Runnable runnable = null;
        if (depth >= 1 && excludes.stream().noneMatch(url::contains)) {
            runnable = () -> {
                try {
                    Document result = downloader.download(url);
                    results.add(url);
                    phaser.register();
                    Runnable r = newExctract(result, excludes, extracted, visited, phaser);
                    extractingExecutor.submit(r);
                } catch (IOException e) {
                    errors.put(url, e);
                }
                phaser.arriveAndDeregister();
            };
        } else {
            phaser.arriveAndDeregister();
        }
        return runnable;
    }

    private Runnable newExctract(Document result, Set<String> excludes, Set<String> extracted, Set<String> visited, Phaser phaser) {
        return () -> {
            try {
                for (String url : result.extractLinks()) {
                    if (!visited.contains(url) && excludes.stream().noneMatch(url::contains)) {
                        visited.add(url);
                        extracted.add(url);
                    }
                }
            } catch (IOException ignored) {
            }
            phaser.arriveAndDeregister();
        };
    }

    public static void main(String[] args) {


        if (args == null) {
            throw new IllegalArgumentException("error.args must be not null.");
        }
        if (args.length > 5 || args.length < 1) {
            // А точно ли описание ошибки соответсвует условию выше?
            throw new IllegalArgumentException("error.there must be 5 args.");
        }
        for (String arg : args) {
            if (arg == null) {
                throw new IllegalArgumentException("error. each of args must be not null.");
            }
        }
        int downloaders;
        int extractors;
        int perHost;

        try {
            downloaders = args.length > 2 ? Integer.parseInt(args[2]) : 1;
            extractors = args.length > 3 ? Integer.parseInt(args[3]) : 1;
            perHost = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        try (Crawler crawler = new WebCrawler(new CachingDownloader(1), downloaders, extractors, perHost)) {
            crawler.download(args[0], args.length > 1 ? Integer.parseInt(args[1]) : 1);
        } catch (IOException e) {
            System.err.println("problem with downloader");

        }

    }

}

