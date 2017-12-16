package com.vannman.xchange;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.time.StopWatch;

public class CollectExchangeStats {

    private static final String LOG_ROOT = "/tmp/EELog";
    private static final String LOG_TARGET_DIR = "system_log";
    private static final String LOG_TARGET_FILE = "xchange.log";
    private static final String VALUE_TAG_START = "<price>";
    private static final String VALUE_TAG_END = "</price>";
    private static final String RESULT_FILE = "/tmp/xratestats.txt";

    private static DateFormat format;
    private static Date startDate;
    private static Date endDate;
    private static String currency;

    static {
        format = new SimpleDateFormat("yyyy-MM-dd");
    }

    // jCommander
    @Parameter(names={"--after", "-a"})
    private String afterDate;
    @Parameter(names={"--before", "-b"})
    private String beforeDate;
    @Parameter(names={"--currency", "-c"})
    private String inCurrency;

    public static void main(String args[]) throws IOException, ParseException{
        StopWatch sw = StopWatch.createStarted();

        CollectExchangeStats main = new CollectExchangeStats();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();

        sw.stop();
        System.out.println("Total elapsed time: " + sw.toString());
    }

    private void run() throws IOException, ParseException {
        // Input option validation
        if (afterDate == null || beforeDate == null || inCurrency == null) {
            System.out.println("Please provide: -a afterDate, -b beforeDate -c currency!");
            return;
        } else {
            startDate = format.parse(afterDate);
            endDate = format.parse(beforeDate);
            currency = inCurrency;
        }

        processLogs();

    }

    private static void processLogs() throws IOException {
        // Traverse log directory structure and process selected files
        List<String> xrates = new ArrayList<>(1000);
        try (DirectoryStream<Path> eeLogStream = Files.newDirectoryStream(Paths.get(LOG_ROOT), entry -> {
                // lambda expression to filter files on given from/to dates
                try {
                    Date fileDate = format.parse(entry.getFileName().toString());
                    return (fileDate.after(startDate) && fileDate.before(endDate));
                }
                catch (ParseException e) {
                    return false;
                }
            })) {
            for (Path dateLog : eeLogStream) {
                try (DirectoryStream<Path> dateLogStream = Files.newDirectoryStream(dateLog, LOG_TARGET_DIR)) {
                    for (Path systemLog : dateLogStream) {
                        try (DirectoryStream<Path> xchangeLogStream = Files.newDirectoryStream(systemLog, LOG_TARGET_FILE)) {
                            for (Path xchangeLog : xchangeLogStream) {
                                xrates.addAll(getExchangeRates(dateLog.getFileName().toString(), xchangeLog));
                            }
                        }
                    }
                }
            }
            //xrates.sort(Comparator.comparing(e -> e));
            addRatesToStatsFile(xrates);
        }
    }

    private static List<String> getExchangeRates(String fileDate, Path filePath) {
        List<String> xrates = new ArrayList<>(10);

        try (Stream<String> stream = Files.lines(filePath)) {
            List<String> lines = stream.collect(Collectors.toList());
            boolean atCurrency = false;
            for (String line: lines) {
                if (line.contains(currency)) {
                    atCurrency = true;
                } else if (line.contains(VALUE_TAG_START) && atCurrency) {
                    int begin = line.indexOf(VALUE_TAG_START) + 7;
                    int end = line.indexOf(VALUE_TAG_END);
                    xrates.add(fileDate.substring(0, 10) + " " + line.substring(begin, end) + System.lineSeparator());
                    atCurrency = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return xrates;
    }

    private static void addRatesToStatsFile(List<String> xrates) throws IOException {
        try (FileWriter fw = new FileWriter(RESULT_FILE)) {
            try (BufferedWriter bw = new BufferedWriter(fw)) {
                for (String line: xrates) {
                    bw.write(line);
                }
            }
        }
    }

}