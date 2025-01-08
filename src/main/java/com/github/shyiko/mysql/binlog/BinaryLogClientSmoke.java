package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Supplier;

public class BinaryLogClientSmoke {

    public static void main(String[] args) throws Exception {
        if (args.length < 14) {
            throw new RuntimeException("Should provide at least 14 arguments, like: " +
                "--host localhost --port 3306 --username root --password root --binlogFile mysql-bin.000135 --binlogPosition 4 --printEventData true");
        }

        String host = args[1];
        int port = Integer.parseInt(args[3]);
        String username = args[5];
        String password = args[7];
        String binlogFile = args[9];
        long binlogPosition = Long.parseLong(args[11]);
        boolean printEventData = Boolean.parseBoolean(args[13]);

        BinaryLogClient binaryLogClient = new BinaryLogClient(host, port, username, password);
        binaryLogClient.setBinlogFilename(binlogFile);
        binaryLogClient.setBinlogPosition(binlogPosition);
        TraceEventListener traceEventListener = new TraceEventListener(binlogFile, printEventData, () -> binaryLogClient);
        binaryLogClient.registerEventListener(traceEventListener);
        binaryLogClient.setHeartbeatInterval(48000);
        binaryLogClient.connect();
    }

    static class TraceEventListener implements BinaryLogClient.EventListener {
        private final String binlogFile;
        private final boolean printEventData;
        private final Supplier<BinaryLogClient> clientSupplier;
        private String currentBinlogFile;

        public TraceEventListener(String binlogFile, boolean printEventData, Supplier<BinaryLogClient> clientSupplier) {
            this.binlogFile = binlogFile;
            this.printEventData = printEventData;
            this.clientSupplier = clientSupplier;
            this.currentBinlogFile = binlogFile;
        }

        @Override
        public void onEvent(Event event) {
            if (event.getData() instanceof RotateEventData) {
                currentBinlogFile = ((RotateEventData) event.getData()).getBinlogFilename();
                System.out.println("Received: " + event + " from " + currentBinlogFile);
                return;
            }

            EventHeaderV4 header = event.getHeader();
            if (currentBinlogFile.equals(binlogFile)) {
                String tableName = parseTableName(event);
                String utcDatetimeStr = Instant.ofEpochMilli(header.getTimestamp()).toString();
                String message = String.format(
                    "Received: current pos: %d, next pos: %d, timestamp: %d %s, %s",
                    header.getPosition(), header.getNextPosition(), header.getTimestamp(), utcDatetimeStr, parseEventType(event));
                if (tableName != null) {
                    message += ", tableName: " + tableName;
                }
                if (printEventData && event.getData() != null) {
                    message += ", eventData: " + event.getData();
                }
                System.out.println(message);
            } else {
                try {
                    clientSupplier.get().disconnect();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private String parseTableName(Event event) {
            if (event.getData() instanceof TableMapEventData) {
                TableMapEventData tableMapEventData = event.getData();
                return tableMapEventData.getDatabase() + "." + tableMapEventData.getTable();
            }
            return null;
        }

        private String parseEventType(Event event) {
            String eventType = event.getHeader().getEventType().toString();
            EventData eventData = event.getData();
            if (eventData instanceof QueryEventData) {
                return eventType + ": " + ((QueryEventData) eventData).getSql();
            }
            return eventType;
        }
    }
}
