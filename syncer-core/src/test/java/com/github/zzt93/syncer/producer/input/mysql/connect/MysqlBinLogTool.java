package com.github.zzt93.syncer.producer.input.mysql.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import lombok.NoArgsConstructor;
import org.junit.Assert;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class MysqlBinLogTool {

	public static void listEventFromMysql() throws Exception {
		BinaryLogClient client = new BinaryLogClient(System.getenv("HOST"), 3306,
				System.getenv("USER"), System.getenv("PWD"));
		client.registerLifecycleListener(new LogLifecycleListener());
		client.setEventDeserializer(SyncDeserializer.defaultDeserializer());
		client.setServerId(1234);
		client.setSSLMode(SSLMode.DISABLED);
		client.setBinlogFilename("mysql-bin.000693");
		client.setBinlogPosition(0);
		client.registerEventListener(new BinLogConsoleWriter());
		client.connect();
	}

	public static void main(String[] args) throws Exception {
		listEventFromMysql();
//    listEventFromFile();
	}

	public static void listEventFromFile() throws Exception {
		EventDeserializer eventDeserializer = SyncDeserializer.defaultDeserializer();
		Event event;
		BinLogConsoleWriter consoleWriter = new BinLogConsoleWriter();
		List<Path> files = Files.list(Paths.get("/data")).filter(s -> s.toString().startsWith("/data/mysql-bin.00294")).collect(Collectors.toList());
		for (Path file : files) {
			try (BinaryLogFileReader reader = new BinaryLogFileReader(file.toFile(), eventDeserializer)) {
				while ((event = reader.readEvent()) != null) {
					consoleWriter.onEvent(event);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				Assert.assertFalse(false);
			}
		}
	}

	@NoArgsConstructor
	public static class BinLogConsoleWriter implements BinaryLogClient.EventListener {
		private final Event[] last = new Event[1];
		private String table = "fund_account";
		private long rowValue = 13271603L;
		private int rowIndex = 3;

    public BinLogConsoleWriter(String table, long rowValue, int rowIndex) {
      this.table = table;
      this.rowValue = rowValue;
      this.rowIndex = rowIndex;
    }

    @Override
		public void onEvent(Event event) {
			EventType eventType = event.getHeader().getEventType();
			if (last[0] != null && ((TableMapEventData) last[0].getData()).getTable().equals(table)) {
				if (EventType.isWrite(eventType)) {
					for (Serializable[] row : ((WriteRowsEventData) event.getData()).getRows()) {
						if (row[rowIndex].equals(rowValue)) {
							System.out.println(last[0]);
							System.out.println(event);
						}
					}
				} else if (EventType.isUpdate(eventType)) {
					List<Entry<Serializable[], Serializable[]>> rows = ((UpdateRowsEventData) event.getData()).getRows();
					for (Entry<Serializable[], Serializable[]> row : rows) {
						if (row.getKey()[rowIndex].equals(rowValue) || row.getValue()[rowIndex].equals(rowValue)) {
							System.out.println(last[0]);
							System.out.println(event);
							System.out.println(getUpdated(row));
						}
					}
				}
			}
			if (eventType == EventType.TABLE_MAP) {
				last[0] = event;
			}
		}

    public static Set<String> getUpdated(Entry<Serializable[], Serializable[]> row) {
      HashSet<String> updated = new HashSet<>(row.getKey().length);
      for (int i = 0; i < row.getKey().length; i++) {
        if (!Objects.deepEquals(row.getKey()[i], row.getValue()[i])) {
          updated.add("" + i + ":" + row.getKey()[i] + "->" + row.getValue()[i]);
        }
      }
      return updated;
    }
	}
}