package com.example.csv_reader;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.opencsv.CSVReader;

public class JavaDemo {
    public static void main(String[] args) {
        String csvFilePath = "D:\\Genesis_Pro_Spring\\CAR_DETAILS_DATA.csv";

        ExecutorService executor = Executors.newFixedThreadPool(5);
        // Q1 Number of data points (rows) and number of attributes (columns) in the
        // dataset
        executor.submit(() -> calRowCol(csvFilePath));
        // Q2 Find number of cars for each category of transmission - Manual or
        // Automatic
        executor.submit(() -> findCat(csvFilePath));
        // Q3 Find the average, minimum and maximum selling price.
        executor.submit(() -> getPriceStatistics(csvFilePath));
        // Q4 Show details of a Car unit based on model name provided as input
        executor.submit(() -> getCarDetailsByModel("Maruti 800 AC", csvFilePath));
        // Q5 Find count of vehicles for each value of year columns. Hint: Find number
        // of vehicle models released each in the year
        executor.submit(() -> getYearCounts(csvFilePath));

        executor.shutdown();

    }

    public static void log(String message) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
        System.out.println(
                "[" + Thread.currentThread().getName() + " " + dtf.format(LocalDateTime.now()) + "] " + message);
    }

    public static void calRowCol(String csvFilePath) {
        log("Started cal_row_col");
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] nextLine;
            boolean first = true;

            int row = 0;
            while ((nextLine = reader.readNext()) != null) {
                if (first) {
                    System.out.println("Number of column: " + nextLine.length);
                    first = false;
                }
                row++;
            }
            row -= -1;
            System.out.println("Number of row: " + row);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CSV validation error: " + e.getMessage());
        }
    }

    public static void findCat(String csvFilePath) {
        log("Started find_cat");
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            HashMap<String, Integer> map = new HashMap<>();
            String[] nextLine;
            boolean firstAuto = true;
            boolean firstManu = true;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine[6].equals("Automatic")) {
                    if (firstAuto) {
                        map.put("Automatic", 1);
                        firstAuto = false;
                    }
                    map.put("Automatic", map.get("Automatic") + 1);
                } else {
                    if (firstManu) {
                        map.put("Manual", 1);
                        firstManu = false;
                    }
                    map.put("Manual", map.get("Manual") + 1);
                }
            }
            for (Map.Entry<String, Integer> en : map.entrySet()) {
                System.out.println("Number of car with " + en.getKey() + " transmission are " + en.getValue());

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("CSV validation error: " + e.getMessage());
        }
    }

    public static void getYearCounts(String csvFilePath) {
        log("Started getYearCounts");
        List<String[]> data;

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            data = reader.readAll();
            Map<String, Integer> counts = new HashMap<>();
            for (String[] row : data.subList(1, data.size())) {
                String year = row[1];
                counts.put(year, counts.getOrDefault(year, 0) + 1);
            }
            System.out.println(counts);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("CSV validation error: " + e.getMessage());
        }

    }

    public static void getPriceStatistics(String csvFilePath) {
        log("Started getPriceStatistics");
        List<Double> prices = new ArrayList<>();
        List<String[]> data;

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath));) {
            data = reader.readAll();
            for (String[] row : data.subList(1, data.size())) {

                double price = Double.parseDouble(row[2]); // Assuming price is at index 2
                prices.add(price);

            }
            if (prices.isEmpty()) {
                System.out.println("Average: 0");
                System.out.println("Minimum: 0");
                System.out.println("Maximum: 0");
            }
            double sum = prices.stream().mapToDouble(Double::doubleValue).sum();
            double min = Collections.min(prices);
            double max = Collections.max(prices);
            double average = sum / prices.size();

            System.out.println("Minimum Price: " + min);
            System.out.println("Maximum Price: " + max);
            System.out.println("Average Price: " + average);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.out.println("Number not valid");
        }

    }

    public static void getCarDetailsByModel(String model, String csvFilePath) {
        log("Started getCarDetailsByModel");
        List<String[]> data;
        Boolean flag = true;
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath));) {
            data = reader.readAll();
            for (String[] row : data.subList(1, data.size())) {
                if (row[0].equalsIgnoreCase(model)) {
                    System.out.println(String.join(", ", row));
                    flag = false;
                }
            }
            if (flag) {
                System.out.println("Model not found");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.out.println("Number not valid");
        }

    }

}
