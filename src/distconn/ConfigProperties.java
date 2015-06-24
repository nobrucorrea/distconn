/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Bruno
 */
public class ConfigProperties {
    
    public  Config getPropValues() {

        try {
            java.util.Properties properties = new java.util.Properties();
            

            InputStream is = new FileInputStream("config.properties");
                    //getClass().getResourceAsStream("../config.properties");

            properties.load(is);


            if (is == null) {

                System.out.println("The config.properties file  does not exist");
            }


            Config config = new Config();
            config.setAlyaHost(properties.getProperty("ALYA_HOST"));
            
            config.setAdanHost(properties.getProperty("ADAN_HOST"));
           
            config.setAdanUserHost(properties.getProperty("ADAN_USER_HOST"));
            config.setAlyaUserHost(properties.getProperty("ALYA_USER_HOST"));
            config.setPrivatekey(properties.getProperty("PRIVATE_KEY"));
            
            config.setAdanFile(properties.getProperty("ADAN_FILE"));
            config.setAlyaFile(properties.getProperty("ALYA_FILE"));
            
            config.setAlyaRepositoryLocal(properties.getProperty("ALYA_REPOSITORY_LOCAL"));
            config.setAdanRepositoryLocal(properties.getProperty("ADAN_REPOSITORY_LOCAL"));
            config.setAlyaInputDiretory(properties.getProperty("ALYA_INPUT_DIRECTORY"));
            config.setAdanInputDiretory(properties.getProperty("ADAN_INPUT_DIRECTORY"));
            
            config.setAdanThreadSleep(Long.parseLong(properties.getProperty("ADAN_THREAD_SLEEP")) );
            config.setAlyaThreadSleep(Long.parseLong(properties.getProperty("ALYA_THREAD_SLEEP")) );

            return config;


        } catch (IOException ex) {
            Logger.getLogger(ConfigProperties.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }


        return null;
    }

    
    
}
