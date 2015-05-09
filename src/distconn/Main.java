/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Bruno
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
          //criando repositorios
       boolean mkdir = new File("alya-repository").mkdir();    
       boolean mkdir1 = new File("adan-repository").mkdir();
        
        try {
            
            Config config = new ConfigProperties().getPropValues();
            new AlyaConnector(config).start();
            
            new ADANConnector(config).start();
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    }
    

