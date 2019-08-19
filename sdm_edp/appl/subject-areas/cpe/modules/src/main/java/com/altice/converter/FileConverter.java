package com.altice.converter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

public class FileConverter {
    @SuppressWarnings("deprecation")
    public static void main(String args[]) throws IOException {
        ArrayList<ArrayList<String>> allRowAndColData = null;
        ArrayList<String> oneRowData = null;
       // String fName = "D:\\Sonali\\CableVision\\Cloudera Migration Proj\\CPE\\CpeWeekly20170313.txt";
        String fName="/home/ksingh6/UTIL/outgoing/cpe/weekly/CpeWeekly20170313.txt";
        String currentLine;
        FileInputStream fis = new FileInputStream(fName);
        DataInputStream myInput = new DataInputStream(fis);
        int i = 0;
        allRowAndColData = new ArrayList<ArrayList<String>>();
        while ((currentLine = myInput.readLine()) != null) {
            oneRowData = new ArrayList<String>();
            String oneRowArray[] = currentLine.split("\\|");
            for (int j = 0; j < oneRowArray.length; j++) {
                oneRowData.add(oneRowArray[j]);
            }
            allRowAndColData.add(oneRowData);
            System.out.println(i);
            i++;
        }

     try {
         HSSFWorkbook workBook = new HSSFWorkbook();
         HSSFSheet sheet = workBook.createSheet("sheet1");
         for (int j = 0; j < allRowAndColData.size(); j++) {
           ArrayList<?> ardata = (ArrayList<?>) allRowAndColData.get(j);
           HSSFRow row = sheet.createRow((short) 0 + j);
           for (int k = 0; k < ardata.size(); k++) {
                System.out.print(ardata.get(k));
                HSSFCell cell = row.createCell((short) k);
                cell.setCellValue(ardata.get(k).toString());
           }
           System.out.println();
         }
       //FileOutputStream fileOutputStream =  new FileOutputStream("D:\\Sonali\\CableVision\\Cloudera Migration Proj\\CPE\\CpeWeekly20170313.xls");
         FileOutputStream fileOutputStream =  new FileOutputStream("/home/ksingh6/UTIL/outgoing/cpe/weekly/CpeWeekly20170313.xls");
       workBook.write(fileOutputStream);
       fileOutputStream.close();
    } catch (Exception ex) {
   }
 }
}