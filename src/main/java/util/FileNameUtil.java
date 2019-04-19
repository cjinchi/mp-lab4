package util;

public class FileNameUtil {
    public static String getPrefix(String fileName) {
        return fileName.substring(0,fileName.length()-14);
    }

    public static void main(String[] args) {
        System.out.println(getPrefix("金庸.txt.segmented"));
    }
}
