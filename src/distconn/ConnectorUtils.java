/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author Bruno
 */
public class ConnectorUtils {
    
    public static List<File> getLastFiles(String dir) {

        File directory = new File(dir);
        File[] files = directory.listFiles();

        Arrays.sort(files, new Comparator<File>() {

            public int compare(File f1, File f2) {
                return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
        });
        
        List<File> fs = new ArrayList();
        //no mac preciso elimiar os arquivops ds.store por isso pego 1 e 2
        fs.add(files[1]);
        fs.add(files[2]);
        
        return fs;

    }
    
    public static boolean compareFiles(File f1 , File f2) {
        
        byte[] f1_buf = new byte[1048576];
        byte[] f2_buf = new byte[1048576];
        if (f1.length() == f2.length()) {
            try {
                InputStream isf1 = new FileInputStream(f1);
                InputStream isf2 = new FileInputStream(f2);
                try {
                    while (isf1.read(f1_buf) >= 0) {
                        isf2.read(f2_buf);
                        for (int j = 0; j < f1_buf.length; j++) {
                            if (f1_buf[j] != f2_buf[j]) {
                                return false;
                            }
                        }
                    }
                } catch (IOException e) {
                }
            } catch (FileNotFoundException e) {
            }
        } else {
            return false; // tamanho e conteudo diferente  
        }
        return true; // arquivos iguais  
    }
    
    public static void removeRemoteFile(Connection con, String pathFile) throws IOException{

         Session ss = con.openSession();
           
         ss.execCommand("rm " + pathFile);
    
    }
    
    
}
