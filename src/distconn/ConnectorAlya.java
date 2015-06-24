package distconn;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import distconn.ADANConnector;
import distconn.Config;
import distconn.ConnectorUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author bcorrea
 */
public class ConnectorAlya extends Thread{
    
    //config
    Config config;

    //SSH
    SCPClient scpADAN;
    SCPClient scpAlya;
    Connection conADAN;
    Connection conAlya;
    File key;

    //Logginng
    Logger LOGGER = null;
    Handler fileHandler = null; 
    boolean firstFile = false;
    
    
    public ConnectorAlya(Config config){
         try {

            LOGGER = Logger.getLogger(ConnectorAlya.class.getName());
            fileHandler = new FileHandler("alya_connector.log");
            LOGGER.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);

            this.config = config;

            conADAN = new Connection(this.config.getAdanHost());
            conAlya = new Connection(config.getAlyaHost());

            conADAN.connect();
            conAlya.connect();

            key = new File(config.getPrivatekey());

            boolean isADANAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);
            boolean isAlyaAuthenticated = conAlya.authenticateWithPublicKey(config.getAlyaUserHost(), key, null);

            if (isADANAuthenticated && isAlyaAuthenticated) {

                scpADAN = conADAN.createSCPClient();
                scpAlya = conAlya.createSCPClient();

                String msg = "Alya Connector connected  ADAN machine .... " + config.getAdanHost();
                String msg1 = "Alya Connector connected  Alya machine .... " + config.getAlyaHost();
                LOGGER.log(Level.INFO, msg);
                LOGGER.log(Level.INFO, msg1);

            } else {

                LOGGER.log(Level.INFO, "ALYA CONNECTOR login failure");

            }
        } catch (IOException e) {

            LOGGER.log(Level.WARNING, e.getMessage());
        }
    }
    
    @Override
    public void run(){
    
         while (true) {

            FileOutputStream out = null;

            //verfico se arquivo ja esta disponibilizado pelo ADAN
            if (isThereALyaFile()) { //arquvio ja foi gerado pelo ADAN, faco o download

                try {

                    //pegando arquivo na maquina do adan e
                    //salvando com o nome do horario do download
                    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
                    String fileName = config.getAlyaRepositoryLocal() + timeStamp;
                    out = new FileOutputStream(fileName);
                    
                    LOGGER.log(Level.INFO, "downloading Alya file");
                    scpAlya.get(config.getAlyaFile(), out);
                    out.close();
                    LOGGER.log(Level.INFO, "Finished downloading");                    
                    
                    //Thread.sleep(5000);
                    
                    //carregar o repositorio 
                    //carregando o repositorio com os arquivos baixados do servidor                     
                    File adanDirectory = new File(config.getAlyaRepositoryLocal());
                    
                    //verificando quantos arquivos existem no repositorio
                    //se for apenas 1, envio para o Alya
                    //senao comparo os ultimos e se forem diferentes, envio o ultimo para o alya
                    //se formem iguais nai faco nada
                    if (adanDirectory.listFiles().length > 1  ){
                        
                        //pego os dois ultimos arquivos do diretorio
                         List<File> alyaFiles = ConnectorUtils.getLastFiles(config.getAlyaRepositoryLocal());
                         
                         //se seles sao diferentes, envioo ultimo recebido para o Alya 
                         if (!ConnectorUtils.compareFiles(alyaFiles.get(0), alyaFiles.get(1))){
                             
                              
                            LOGGER.log(Level.INFO, "Different files,  sending to ADAN");
                            scpADAN.put(alyaFiles.get(1).getAbsolutePath(), "AL2C.temp", config.getAlyaInputDiretory(), "0755" );
                            LOGGER.log(Level.INFO, "File sent to ADAN");

                            //apagar arquivo adan- preciso confrmar
                            //ConnectorUtils.removeRemoteFile(conADAN, config.getAdanFile());
                            alyaFiles.get(0).delete();
                            
                             
                         }else{// se sao os mesmos nao faco nada
                            
                            //String msg = "Comparamos " + adanFiles.get(0).getName() + " com " + adanFiles.get(1).getName();
                            LOGGER.log(Level.INFO, "Same files, dont send to ADAN");
                            //LOGGER.log(Level.INFO, msg);
                            
                            //Se ele é mesmo que ja tenho na base posso apaga-lo
                            File rf = new File(fileName);
                            rf.delete();
                            LOGGER.log(Level.INFO, "same file deleted");
                         
                         }
                         
                        
                              
                       
                        
                        
                     
                    }else{ //só existe um arquivo no repositorio, envio eireto para o Alya
                     
                        if(!firstFile){
                            File file = new File(config.getAlyaRepositoryLocal());
                            LOGGER.log(Level.INFO, "Sending first file to ADAN");
                            File f = file.listFiles()[0];
                            System.out.println(file.listFiles()[0].getAbsolutePath());
                            scpADAN.put(f.getAbsolutePath(), "AL2C.temp",  config.getAlyaInputDiretory(),  "0755");
                            LOGGER.log(Level.INFO, "File sent to ADAN");
                            firstFile = true;
                        }
                    }
                    
                } catch (FileNotFoundException ex) {
                    
                   LOGGER.log(Level.WARNING, ex.getMessage());
                } catch (IOException ex) {
                    LOGGER.log(Level.WARNING, ex.getMessage());
                }

            }else{
          
                LOGGER.log(Level.INFO,"There is not ADAN AL2C.temp");
                
                
            }
            
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                LOGGER.log(Level.INFO, ex.getMessage());
            }
        }
    
    }
    
    
     private boolean isThereALyaFile() {

        boolean isFile = false;
        Session sess = null;

        try {

            sess = conAlya.openSession();
            //config.getAdanFile().substring(0,config.getAdanFile().lastIndexOf("/"));
            String command  = "ls " + config.getAlyaFile().substring(0, config.getAlyaFile().lastIndexOf("/"));
            LOGGER.log(Level.INFO, command);
            sess.execCommand(command);
            InputStream stdout = new StreamGobbler(sess.getStdout());
            String line = null;
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));

            while (true) {

                line = br.readLine();

                if (line == null) {

                    break;

                } else {

                    if (line.contains("AL2C.temp")) {
                        isFile = true;
                        LOGGER. log(Level.WARNING, line);
                        break;
                    }
                }
            }

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());        
        
        }

        sess.close();
        return isFile;

    }

    
}
