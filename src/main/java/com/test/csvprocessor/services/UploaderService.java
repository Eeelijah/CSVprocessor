package com.test.csvprocessor.services;

import com.test.csvprocessor.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class UploaderService {

    @Autowired
    private Configuration conf;

    private Logger log = Logger.getLogger(UploaderService.class);

    public void processNewFiles(String user, String pwd, String DBName) {
        log.info("Начало очередной обработки.");
        File[] files = new File(conf.getInputDir()).listFiles();

        if (!hasNewFiles(files)) {
            log.info("Необработанных файлов нет.");
            return;
        }

        List<String> duplicates = deleteDuplicates(files);
        log.info("Удалено " + duplicates.size() + " дублей.");
        for (String name : duplicates) {
            log.info("Удален: " + name);
        }

        if (new File(conf.getInputDir()).listFiles().length == 0) {
            log.info("После удаления дублей необработанных файлов не осталось.");
            return;
        }

        try {
            Class.forName(conf.getJDBC_DRIVER());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        createDB(user, pwd, DBName);
        createUser(user, pwd, DBName);

        for (File file : files) {
            uploadToDB(file, user, pwd, DBName);
            moveProcessedFile(file);
        }
        log.info("Обработка завершена.");
    }



    private boolean hasNewFiles(File[] files) {
        return files.length > 0;
    }

    private List<String> deleteDuplicates(File[] files) {
        List<String> duplicates = new ArrayList<>();
        for (File file : files) {
            if (!isFileProcessed(file)) {
                continue;
            }
            duplicates.add(file.getName());
            file.delete();
        }
        return duplicates;
    }

    private boolean isFileProcessed(File file) {
        return new File(conf.getOutputDir(), file.getName()).exists();
    }

    private void createDB(String user, String pwd, String DBName) {

        try (Connection conn = DriverManager.getConnection(conf.getUrl(), user, pwd);
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE DATABASE IF NOT EXISTS " + DBName;
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            log.error("Не удалось создать Базу Данных", e);
            throw new RuntimeException("Ошибка при создании БД: " + e, e);
        }
    }

    private void createUser(String name, String pwd, String DBName) {

        try (Connection conn = DriverManager.getConnection(conf.getSysDB(), conf.getUser(), conf.getPassword());
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE USER IF NOT EXISTS '" + name + "'@'localhost' IDENTIFIED BY '" + pwd + "'";
            stmt.executeUpdate(sql);
            sql = "GRANT ALL PRIVILEGES ON " + DBName + ".* TO '" + name + "'@'localhost'";
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            log.error("Не удалось создать пользователя", e);
            throw new RuntimeException("Ошибка при создании пользователя: " + e, e);
        }
    }

    private void uploadToDB(File file, String user, String pwd, String DBName) {

        try (Connection conn = DriverManager.getConnection(conf.getUrl()  + DBName, user, pwd);
             Statement stmt = conn.createStatement()) {

            String insertQuery = null;
            PreparedStatement pstmt = null;
            String[] rowData = null;
            int i = -1;

            List<String> lines = IOUtils.readLines(new FileInputStream(file), "utf-8");
            for (String line : lines) {
                rowData = line.split(",");
                if (i == -1) {
                    String createTable = "CREATE TABLE IF NOT EXISTS `" + file.getName() + "` (`"
                                                                        + rowData[0] + "` int NOT NULL, `"
                                                                        + rowData[1] + "` VARCHAR(255) NOT NULL, `"
                                                                        + rowData[2] + "` double NOT NULL)";

                    insertQuery =  "Insert into `" + file.getName() + "` (" + rowData[0] + "," + rowData[1] + "," + rowData[2] + ") values (?,?,?)";
                    stmt.executeUpdate(createTable);

                    pstmt = conn.prepareStatement(insertQuery);
                    i++;
                    continue;
                }

                for (String data : rowData) {

                    pstmt.setString((i % 3) + 1, data);

                    if (++i % 3 == 0) {
                        pstmt.addBatch();
                    }
                    if (i % 10 == 0) {
                        pstmt.executeBatch();
                    }
                }
            }
            log.info("Обработан файл " + file.getName() + "; Добавлено " + (lines.size() - 1) + " записей");
        } catch (Exception e) {
            log.error("Не удалось обработать файл: " + file.getName(), e);
            throw new RuntimeException("Ошибка при обработке файла: " + file.getName() + ";\t" + e, e);
        }
    }

    private void moveProcessedFile(File file) {

        try (InputStream is = new FileInputStream(file);
             OutputStream os = new FileOutputStream(new File(conf.getOutputDir(), file.getName()))){
            IOUtils.copy(is, os);
        } catch (IOException e) {
            log.error("Не удалось переместить обработанный файл: " + file.getName(), e);
            throw new RuntimeException("Ошибка при перемещении файла: " + file.getName() + ";\t" + e, e);
        } finally {
            file.delete();
        }
    }
}
