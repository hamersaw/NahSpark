package com.bushpath.atlas.spark;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Scanner;

public class LineParser {
    protected Scanner scan;

    public LineParser(InputStream in) {
        this.scan = new Scanner(new BufferedInputStream(in), "UTF-8");
    }

    public String nextRecord() {
        if (!this.scan.hasNextLine()) {
            return null;
        }

        return this.scan.nextLine();
    }

    /*protected BufferedInputStream in;

    public LineParser(InputStream in) {
        this.in = new BufferedInputStream(in);
    }

    public String nextRecord() {
        StringBuilder builder = new StringBuilder();
        try {
            while (true) {
                byte b = (byte) this.in.read();
                if (b == '\n' || b == -1) {
                    break;
                }

                builder.append((char) b);
            }
        } catch (IOException e) {
        }

        if (builder.length() == 0) {
            return null;
        } else {
            return builder.toString();
        }
    }*/
}
