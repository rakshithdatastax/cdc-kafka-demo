package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.cdcdemo.config.DemoProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BackfillDemoService {

    private final CqlSession session;
    private final DemoProperties props;

    public String createTableCdcDisabled() {
        DemoProperties.Backfill config = props.getBackfill();
        session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                        + "{'class': 'NetworkTopologyStrategy', 'dc1': 3}",
                config.getKeyspace()));
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, val int) WITH cdc = false",
                config.getKeyspace(), config.getTable()));
        return qualifiedTable() + " created with cdc=false";
    }

    public String seed(int count) {
        String qualified = qualifiedTable();
        for (int i = 0; i < count; i++) {
            session.execute(
                    String.format("INSERT INTO %s (id, val) VALUES (?, ?)", qualified),
                    "bf-" + i, i);
        }
        return "inserted " + count + " rows into " + qualified + " while cdc was disabled";
    }

    public String enableCdc() {
        session.execute(String.format("ALTER TABLE %s WITH cdc = true", qualifiedTable()));
        return qualifiedTable() + " altered to cdc=true";
    }

    private String qualifiedTable() {
        DemoProperties.Backfill config = props.getBackfill();
        return config.getKeyspace() + "." + config.getTable();
    }
}
