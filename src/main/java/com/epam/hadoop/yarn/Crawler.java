package com.epam.hadoop.yarn;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;

import java.io.*;
import java.util.*;

public class Crawler {

    private static final Log LOG = LogFactory.getLog(Crawler.class);

    public static final int TOP_KEYWORDS_AMOUNT = 10;
    public static final int COLUMN_AMOUNT = 6;
    public static final int URL_INDEX = 5;
    public static final int KEYWORDS_INDEX = 1;

    private Configuration conf;
    private Path inputFilePath;
    private Path outpuFilePath;;

    public Crawler(String inputFile, String outputFile) {
        conf = new Configuration();
        inputFilePath = new Path(inputFile);
        outpuFilePath = new Path(outputFile);
    }

    public void run() throws IOException {

        String line = null;
        int linesRead = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(inputFilePath)));
        try {
            PrintWriter writer = new PrintWriter(FileSystem.get(conf).create(outpuFilePath));
            try {
                while ((line = reader.readLine()) != null) {
                    linesRead++;

                    //skip first line with headers
                    if (linesRead == 1) {
                        //write header
                        writer.println(line);
                        continue;
                    } else {
                        LOG.debug("LINE:" + line);

                        String[] columnValues = splitLineIntoColumnValues(line);
                        if (columnValues.length != COLUMN_AMOUNT) {
                            LOG.error("Line #" + linesRead + " '" + line + "' can't be parsed correctly. Skipping...");
                        } else {
                            String pageURL = columnValues[URL_INDEX];

                            if (StringUtils.isNotBlank(pageURL)) {
                                List<String> keywordsList = getKeywordsFromPage(pageURL);

                                if (!keywordsList.isEmpty()) {
                                    List<Map.Entry<String, Integer>> entryList = sortKeywordsByFequency(keywordsList);

                                    LOG.debug("Top keywords");
                                    Set<String> topKeywordsSet = new LinkedHashSet<>();
                                    for (int i = 0; i < entryList.size() && i < TOP_KEYWORDS_AMOUNT; i++) {
                                        LOG.debug(i + " " + entryList.get(i));
                                        topKeywordsSet.add(entryList.get(i).getKey());
                                    }
                                    LOG.debug(topKeywordsSet);
                                    String keywordsColumnValue = StringUtils.join(topKeywordsSet, ',');
                                    columnValues[KEYWORDS_INDEX] = keywordsColumnValue;

                                    //write to output file
                                    writer.println(StringUtils.join(columnValues, '\t'));
                                } else {
                                    LOG.error("No keyword was found. See previous errors");
                                }
                            } else {
                                LOG.error("Error reading line #" + linesRead + ": Empty url value");
                            }
                        }
                    }
                }
            } finally {
                IOUtils.closeStream(writer);
            }
        } finally {
            IOUtils.closeStream(reader);
        }

    }

    private String[] splitLineIntoColumnValues(String line) {
        return line.split("\\t");
    }

    private List<Map.Entry<String, Integer>> sortKeywordsByFequency(List<String> keywordsList) {
        Set<String> keywordsSet = new TreeSet(keywordsList);
        Map<String, Integer> keywordToCountMap = new HashMap();
        for (String keyword : keywordsSet) {
            int count = Collections.frequency(keywordsList, keyword);
            keywordToCountMap.put(keyword, count);
        }

        List<Map.Entry<String, Integer>> entryList = new ArrayList(keywordToCountMap.entrySet());
        Collections.sort(entryList,
                (Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) -> entry2.getValue().compareTo(entry1.getValue()));
        return entryList;
    }

    private List<String> getKeywordsFromPage(String pageURL) {
        List<String> keywordsList = ListUtils.EMPTY_LIST;
        try {
            Document doc = Jsoup.connect(pageURL)
                    .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0")
                    .referrer("http://ww.google.com")
                    .parser(Parser.xmlParser())
                    //default is to 3000 ms, low to 2000 ms
                    .timeout(2000)
                    .get();
            //remove script tag and it's content
            doc.select("script").remove();

            //get text from document
            String text = doc.body().text();
            //clear text from non-alphabet symbols and single letter and split it by whitespace symbols
            keywordsList = Arrays.asList(text.replaceAll("[^a-zA-Z]", " ").toLowerCase().replaceAll("\\s[a-z]\\s", " ").split("\\s+"));
        } catch (IOException e) {
            LOG.error("Request for url: " + pageURL + " failed.", e);
        }

        return keywordsList;
    }

    public static void main(String[] args) throws Exception {
        Crawler crawler = new Crawler(args[0], args[1]);
        crawler.run();
    }
}
