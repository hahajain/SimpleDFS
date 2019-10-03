import java.util.*;
import java.util.ArrayList;
import java.util.Collections;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


class Module{
  String fileName;
  String rw;
  Module(String rw, String fileName){
      this.rw = rw;
      this.fileName = fileName;
  }
}


/*
   Class FileGen:
   1. Helps to generate a file with a specified number of read and write requests
   2. The number of files can be specified
   3. Does a rrandom generation
   4. Shuffles the requests and writes it to a file in TestFiles folder
*/
class FileGen{
    public static void main(String[] args){
        ArrayList<Module> arrList = new ArrayList<Module>();
        int total = 2000;
        int nr = 2000;
        int nw = 0;
        while(nr-- > 0){
            Random rand = new Random();
            int n = rand.nextInt(50)+1;
            Module m = new Module("r","file"+n+".txt");
            arrList.add(m);
        }
        while(nw-- > 0){
            Random rand = new Random();
            int n = rand.nextInt(50)+1;
            Module m = new Module("w","file"+n+".txt");
            arrList.add(m);
        }
        Collections.shuffle(arrList);
        try {
              File file = new File("./TestFiles/testFile100_0.txt");
              FileWriter fileWriter = new FileWriter(file);
              for(Module m : arrList){
                  fileWriter.write(m.rw+" "+m.fileName+" "+"\n");
              }
              fileWriter.flush();
              fileWriter.close();
        } catch (IOException e) {e.printStackTrace();}
    }
}
