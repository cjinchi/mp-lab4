package app;

import util.HBaseController;

import java.io.IOException;

public class HBaseToLocalFile {
    public static void main(String[] args) throws IOException {
        HBaseController.saveToLocalFile("average_count.csv");
    }
}
