package world.data.jdbc.internal.util;

import lombok.extern.java.Log;

import java.sql.SQLWarning;

@Log
public class WarningList {
    private SQLWarning head, tail;

    public synchronized SQLWarning get() {
        return head;
    }

    public void add(String warning) {
        add(new SQLWarning(warning));
    }

    public synchronized void add(SQLWarning warning) {
        log.warning("SQL Warning was issued: " + warning);
        if (head == null) {
            head = warning;
        } else {
            tail.setNextWarning(warning); // chain with existing warnings
        }
        tail = warning;
    }

    public synchronized void clear() {
        head = tail = null;
    }
}
