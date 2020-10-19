package no.ssb.rawdata.converter.util;

import com.google.common.base.Strings;

public final class RuntimeVariables {

    public static String memory() {
        return debugItem("maxMemory", Runtime.getRuntime().maxMemory())
                + debugItem("totalMemory", Runtime.getRuntime().totalMemory())
                + debugItem("freeMemory", Runtime.getRuntime().freeMemory());
    }

    private static String debugItem(String label, Object value) {
        return Strings.padEnd(label, 24, '.') + " " + value + "\n";
    }
}
