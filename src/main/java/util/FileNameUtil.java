package util;

public class FileNameUtil {
    public static String getPrefix(String fileName) {
        if (fileName.endsWith(".txt.segmented")){
            return fileName.substring(0,fileName.length()-14);
        }else{
            return fileName;
        }
    }

    public static void main(String[] args) {
        System.out.println(getPrefix("金庸.txt.segmented"));
    }
}
