package com.axibase.tsd.driver.jdbc.examples;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static util.TestProperties.LOGIN_NAME;
import static util.TestProperties.LOGIN_PASSWORD;

@Slf4j
public class BatchExample {
    private static final String QUERY = "INSERT INTO \"user-dirs-size\" (entity, time, value, tags.directory) VALUES (?,?,?,?)";

    @Test
    public void testGenerateUserDirsBatch() throws Exception {
        try (final Connection connection = DriverManager.getConnection("jdbc:atsd://localhost:8443", LOGIN_NAME, LOGIN_PASSWORD)) {
            final PreparedStatement preparedStatement = connection.prepareStatement(QUERY);

            final long now = System.currentTimeMillis();
            final String hostname = InetAddress.getLocalHost().getHostName();
            final Iterator<Path> iterator = Files.list(Paths.get(System.getProperty("user.home"))).iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    final Path path = iterator.next();
                    if (Files.isDirectory(path)) {
                        final SizeCounterFileVisitor visitor = new SizeCounterFileVisitor();
                        Files.walkFileTree(path, visitor);
                        preparedStatement.setString(1, hostname);
                        preparedStatement.setLong(2, now);
                        preparedStatement.setLong(3, visitor.getSize());
                        preparedStatement.setString(4, path.toString());
                        preparedStatement.addBatch();
                    }
                }
                final int[] updateCount = preparedStatement.executeBatch();
                final int totalCount = IntStream.of(updateCount).sum();
                System.out.println("Updated: " + totalCount);
                assertThat(totalCount, greaterThan(0));
            }
        }
    }

    private static final class SizeCounterFileVisitor extends SimpleFileVisitor<Path> {
        private long totalSize = 0L;

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            log.error("Visit file failed: {}", exc.getMessage());
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            totalSize += attrs.size();
            return FileVisitResult.CONTINUE;
        }

        public long getSize() {
            return totalSize;
        }
    }
}
