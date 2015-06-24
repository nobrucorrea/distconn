/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

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

/**
 *
 * @author Bruno
 */
public class ADANConnector extends Thread {

    //config
    Config config;
    
    //SSH
    SCPClient scpADAN;
    SCPClient scpAlya;
    Connection conADAN;
    Connection conAlya;
    File key;
    
    //Logginng
    Logger LOGGER=null;
    Handler fileHandler=null;
    

    public ADANConnector(Config config) {     
                        

        try {
            
            LOGGER  =Logger.getLogger(ADANConnector.class.getName());
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
             
                String msg  = "ADAN Connector connected  ADAN machine .... " + config.getAdanHost();
                String msg1 = "ADAN Connector connected  Alya machine .... " + config.getAlyaHost();
                LOGGER.log(Level.INFO, msg);
                LOGGER.log(Level.INFO, msg1);

            } else {

                LOGGER.log(Level.INFO,"ADAN CONNECTOR login failure");

            }
        } catch (IOException e) {

            LOGGER.log(Level.WARNING,e.getMessage());
        }

    }

    @Override
    public void run() {

        while (true) {

            FileOutputStream out = null;
            try {

                //pegando arquivo na maquina do alya e
                //salvando com o nome do horario do download
                String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
                out = new FileOutputStream(config.getAdanRepositoryLocal() + timeStamp);
                LOGGER.log(Level.INFO, "downloading ADAN file");
                scpADAN.get(config.getAdanFile(), out);               
                out.close();
                //testar conteudo do arquivo
                
               

                Thread.sleep(5000);
                //carregando o repositorio com os arquivos baixados do servidor 
                System.out.println(config.getAdanRepositoryLocal());
                File adanDirectory = new File(config.getAdanRepositoryLocal());

               
                //testa para ver se ja existe algum arquivo no repositorio
                //caso nao exista nao é preciso comparar apenas enviar para o alya
                if (adanDirectory.listFiles().length > 1  ) {
                    //comparando os arquivos                    
                    List<File> adanFiles = ConnectorUtils.getLastFiles(config.getAdanRepositoryLocal());

                    if (!ConnectorUtils.compareFiles(adanFiles.get(0), adanFiles.get(1))) {
                        
                        LOGGER.log(Level.INFO, "Different files");
                        //Enviar arquivo para alya
                      

                            LOGGER.log(Level.INFO, "Different files,  sending to ALya");
                           // scpAlya.put(adanFiles.get(0).getAbsolutePath(), "C2AL.temp", config.getAlyaInputDiretory(), "0755" );
                            LOGGER.log(Level.INFO, "File sent to ALya");

                            //apagar arquivo adan
                            //ConnectorUtils.removeRemoteFile(conADAN, config.getAdanFile());
                            
                            //apagar arquivos do repositorio que nao sera utiliazados, fico apenas com o ultimo recebido                            
                            adanFiles.get(1).delete();

                    } else {
                        //System.out.println("Arquivos iguais, nao envio");
                        LOGGER.log(Level.INFO, "Same files, dont send to alya");
                        //adanFiles.get(1).delete();
                    }

                } else {

                   
                    Thread.sleep(50000);
                    File file = new File(config.getAdanRepositoryLocal());
                    
                    
                    LOGGER.log(Level.INFO, "Sending first file to Alya");
                    File f = file.listFiles()[0];
                    System.out.println(file.listFiles()[0].getAbsolutePath());
                    //scpAlya.put(f.getAbsolutePath(), "C2AL.temp",  config.getAlyaInputDiretory(),  "0755");
                    LOGGER.log(Level.INFO, "File sent to Alya");
                      

                }

                //System.out.println("Sleeping...");
                 LOGGER.log(Level.INFO, "Sleeping");
                
                Thread.sleep(config.getAdanThreadSleep());
                 
                
                

            } catch (FileNotFoundException ex) {
                //System.out.println(ex.getMessage());
                String msg = ex.getMessage() +"--" +ex.getStackTrace()[0].getLineNumber();
                LOGGER.log(Level.WARNING, msg);
                
                
                
            } catch (InterruptedException ex) {
                //System.out.println(ex.getMessage());
                String msg = ex.getMessage() +"--" +ex.getStackTrace()[0].getLineNumber();
                LOGGER.log(Level.WARNING, ex.getMessage());
                
               
            } catch (IOException ex) {
               //System.out.println(ex.getMessage());
                String msg = ex.getMessage() +"--" +ex.getStackTrace()[0].getLineNumber();
                LOGGER.log(Level.WARNING, ex.getMessage());
                //eraseEmptyFiles();
            } finally {
                
                
            }
            
            

        }

    }

    
    private void eraseEmptyFiles(){
        
         FileInputStream fis = null;
         File dir = new File(config.getAlyaRepositoryLocal());
         
         try {
             String msg;
            for(File file: dir.listFiles()) {
                
                fis = new FileInputStream(file);

                int b = fis.read();

                if (b == -1){
                  msg = "File "+ file.getName() + " empty was deleted";
                  LOGGER.log(Level.INFO, msg);
                  fis.close();
                  file.delete();
                }
            }        
   
            
         } catch (FileNotFoundException ex) {
             LOGGER.log(Level.WARNING, ex.getMessage());
         
         }catch(IOException ex){
              LOGGER.log(Level.WARNING, ex.getMessage());
             
         }
    
        try {
         
                 Thread.sleep(2000);
        } catch (InterruptedException ex) {
            Logger.getLogger(ADANConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }

    
    private boolean isADANFile(){
    
        
          boolean isFile = false;
          
          try{
          
            Session sess = conADAN.openSession();
            
            sess.execCommand("ls");
            InputStream stdout = new StreamGobbler(sess.getStdout());
            String line = null;
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));

            while(true){
            
                line = br.readLine();

                if (line == null) {

                    break;

                } else {

                    if (line.contains("C2Al.temp")) {
                        isFile = true;
                    }
                }
            }

          }catch(IOException ex){
          
          
          }
          
          return isFile;
    
    }
    
    public boolean adanIsRunning(String pid) {

        boolean running = false;

        try {

            System.out.println("checking adan running");
            Session sess = conADAN.openSession();

            sess.execCommand("qstat");

            InputStream stdout = new StreamGobbler(sess.getStdout());
            String line = null;
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));

            while (true) {

                line = br.readLine();

                if (line == null) {

                    break;

                } else {

                    if (line.contains(pid)) {
                        running = true;
                    }
                }

            }
        } catch (IOException e) {
                 Logger.getLogger(AlyaConnector.class.getName()).log(Level.SEVERE, null, e);
        }

        return running;
    }

}
