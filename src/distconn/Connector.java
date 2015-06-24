/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

/**
 *
 * @author bcorrea
 */
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

public class Connector extends Thread {

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

    public Connector(Config config) {

        try {

            LOGGER = Logger.getLogger(ADANConnector.class.getName());
            fileHandler = new FileHandler("connector_adan.log");
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

                String msg = "ADAN Connector connected  ADAN machine .... " + config.getAdanHost();
                String msg1 = "ADAN Connector connected  Alya machine .... " + config.getAlyaHost();
                LOGGER.log(Level.INFO, msg);
                LOGGER.log(Level.INFO, msg1);

            } else {

                LOGGER.log(Level.INFO, "ADAN CONNECTOR login failure");

            }
        } catch (IOException e) {

            LOGGER.log(Level.WARNING, e.getMessage());
        }

    }

    @Override
    public void run() {

        while (true) {

            FileOutputStream out = null;

            //verfico se arquivo ja esta disponibilizado pelo ADAN
            if (isThereADANFile()) { //arquvio ja foi gerado pelo ADAN, faco o download

                try {

                    //pegando arquivo na maquina do adan e
                    //salvando com o nome do horario do download
                    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
                    String fileName = config.getAdanRepositoryLocal() + timeStamp;
                    out = new FileOutputStream(fileName);
                    
                    LOGGER.log(Level.INFO, "downloading ADAN file");
                    scpADAN.get(config.getAdanFile(), out);
                    out.close();
                    LOGGER.log(Level.INFO, "Finished downloading");                    
                    
                    //Thread.sleep(5000);
                    
                    //carregar o repositorio 
                    //carregando o repositorio com os arquivos baixados do servidor                     
                    File adanDirectory = new File(config.getAdanRepositoryLocal());
                    
                    //verificando quantos arquivos existem no repositorio
                    //se for apenas 1, envio para o Alya
                    //senao comparo os ultimos e se forem diferentes, envio o ultimo para o alya
                    //se formem iguais nai faco nada
                    if (adanDirectory.listFiles().length > 1  ){
                        
                        //pego os dois ultimos arquivos do diretorio
                         List<File> adanFiles = ConnectorUtils.getLastFiles(config.getAdanRepositoryLocal());
                         
                         //se seles sao diferentes, envioo ultimo recebido para o Alya 
                         if (!ConnectorUtils.compareFiles(adanFiles.get(0), adanFiles.get(1))){
                             
                              
                            LOGGER.log(Level.INFO, "Different files,  sending to Alya");
                            scpAlya.put(adanFiles.get(1).getAbsolutePath(), "C2AL.temp", config.getAlyaInputDiretory(), "0755" );
                            LOGGER.log(Level.INFO, "File sent to ALya");

                            //apagar arquivo adan- preciso confrmar
                            //ConnectorUtils.removeRemoteFile(conADAN, config.getAdanFile());
                            
                            adanFiles.get(0).delete();
                            
                            
                            
                             
                         }else{// se sao os mesmos nao faco nada
                            
                            //String msg = "Comparamos " + adanFiles.get(0).getName() + " com " + adanFiles.get(1).getName();
                            LOGGER.log(Level.INFO, "Same files, dont send to alya");
                            //LOGGER.log(Level.INFO, msg);
                            
                            //Se ele é mesmo que ja tenho na base posso apaga-lo
                            File rf = new File(fileName);
                            rf.delete();
                            LOGGER.log(Level.INFO, "arquivo igual apagado");
                         
                         }
                         
                        
                              
                       
                        
                        
                     
                    }else{ //só existe um arquivo no repositorio, envio direto para o Alya
                     
                        if(!firstFile){
                            File file = new File(config.getAdanRepositoryLocal());
                            LOGGER.log(Level.INFO, "Sending first file to Alya");
                            File f = file.listFiles()[0];
                            System.out.println(file.listFiles()[0].getAbsolutePath());
                            scpAlya.put(f.getAbsolutePath(), "C2AL.temp",  config.getAlyaInputDiretory(),  "0755");
                            LOGGER.log(Level.INFO, "File sent to Alya");
                            firstFile= true;
                        }
                    }
                    
                } catch (FileNotFoundException ex) {
                    
                   LOGGER.log(Level.WARNING, ex.getMessage());
                } catch (IOException ex) {
                    LOGGER.log(Level.WARNING, ex.getMessage());
                }

            }else{
          
                LOGGER.log(Level.INFO,"There is not ADAN C2AL.temp");
                
                
            }
            
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                LOGGER.log(Level.INFO, ex.getMessage());
            }
        }

    }

    private boolean isThereADANFile() {

        boolean isFile = false;
        Session sess = null;

        try {

            sess = conADAN.openSession();

            String command = "ls "+ config.getAdanFile().substring(0,config.getAdanFile().lastIndexOf("/")); 
            
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

                    if (line.contains("C2AL.temp")) {
                        isFile = true;
                        LOGGER.log(Level.WARNING, line);
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
