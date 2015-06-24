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


public class AlyaConnector extends Thread {

    //Config
    Config config;
    //SSH
    SCPClient scpADAN;
    SCPClient scpAlya;
    Connection conADAN;
    Connection conAlya;
    File key;

    //Logging
    Logger LOGGER = null;
    Handler fileHandler = null;

    public AlyaConnector(Config config) {

        try {

            LOGGER = Logger.getLogger(AlyaConnector.class.getName());
            fileHandler = new FileHandler("connector_alya.log");
            LOGGER.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);

            this.config = config;

            conAlya = new Connection(this.config.getAlyaHost());
            conADAN = new Connection(config.getAdanHost());

            conAlya.connect();
            conADAN.connect();

            key = new File(config.getPrivatekey());

            boolean isAlyaAuthenticated = conAlya.authenticateWithPublicKey(config.getAlyaUserHost(), key, null);
            boolean isADANAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);

            if (isAlyaAuthenticated && isADANAuthenticated) {

                scpAlya = conAlya.createSCPClient();
                scpADAN = conADAN.createSCPClient();
                String msg =  "Alya Connector connected Alya machine .... " + config.getAlyaHost();
                String msg1 = "Alya Connector connected ADAN machine .... " + config.getAdanHost();
                
                LOGGER.log(Level.INFO, msg);
                LOGGER.log(Level.INFO, msg1);
                //System.out.println("Alya Connector connected Alya machine .... " + config.getAlyaHost());
                //System.out.println("Alya Connector connected ADAN machine .... " + config.getAdanHost());

            } else {

               // System.out.println("Login Alya failure");
                LOGGER.log(Level.WARNING,"Login Alya failure" );

            }
        } catch (IOException e) {
            //System.out.println(e.getMessage());
            LOGGER.log(Level.WARNING,e.getMessage() );
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
                out = new FileOutputStream(config.getAlyaRepositoryLocal() + timeStamp);
                
                LOGGER.log(Level.INFO,"Downloading Alya file" );
                scpAlya.get(config.getAlyaFile(), out);
                
                
                out.close();

                //carregando o repositorio com os arquivos baixados do servidor 
                File alyaDirectory = new File(config.getAlyaRepositoryLocal());

                //testa para ver se ja existe algum arquivo no repositorio
                //caso nao exista nao é preciso comparar apenas enviar para o adan
                if (alyaDirectory.listFiles().length > 2) {
                    //System.out.println("Comparando arquivos");
                    //comparando os arquivos                    
                    List<File> alyaFiles = ConnectorUtils.getLastFiles(config.getAlyaRepositoryLocal());

                    if (!ConnectorUtils.compareFiles(alyaFiles.get(0), alyaFiles.get(1))) {

                        LOGGER.log(Level.INFO,"Different files" );
                        //Enviar arquivo para adan
                        /*conADAN = new Connection(config.getAdanHost());
                         conADAN.connect();
                         boolean isADANAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);
                         */
                        //if (isADANAuthenticated) {
                        //scpADAN = conADAN.createSCPClient();
                        LOGGER.log(Level.INFO,"Sending to ADAN" );
                        //scpADAN.put(alyaFiles.get(0).getAbsolutePath(), "AL2C.temp", config.getAdanInputDiretory(), "0755");
                        LOGGER.log(Level.INFO,"File sent to ADAN" );

                            //apagar arquivo alya
                        //ConnectorUtils.removeRemoteFile(conAlya, config.getAdanFile());
                        //}
                    } else {
                        LOGGER.log(Level.INFO, "Same files, dont send to ADAN");
                    }

                } else {

                    Thread.sleep(50000);
                    File file = new File(config.getAlyaRepositoryLocal());
                    //System.out.println("arquivos no diretorio" + file.listFiles().length);
                    //System.out.println("sending first file to adan");
                    LOGGER.log(Level.INFO, "Sending first file to ADAN");

                    /*conADAN = new Connection(config.getAdanHost());
                     conADAN.connect();
                     boolean isAlyaAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);*/
                    //if (isAlyaAuthenticated) {
                    //  scpADAN = conADAN.createSCPClient();
                    File f = file.listFiles()[0];                    
                    System.out.println(file.listFiles()[0].getAbsolutePath());
                    //scpADAN.put(f.getAbsolutePath(), "AL2C.temp", config.getAdanInputDiretory(), "0755");

                    //System.out.println("File sent to ADAN");
                    LOGGER.log(Level.INFO, "File sent to ADAN");

                    /*} else {
                     System.out.println("ADAN authenticated failed");
                     } */
                }

                Thread.sleep(config.getAlyaThreadSleep());
                deleteFiles();

            } catch (FileNotFoundException ex) {
                //System.out.println(ex.getMessage());
                //LOGGER.log(Level.WARNING, ex.getMessage());
                String msg = ex.toString() +"--" +ex.getStackTrace().toString();
                LOGGER.log(Level.WARNING, msg);
            } catch (InterruptedException ex) {
                //System.out.println(ex.getMessage());
                        
                String msg = ex.toString() +"--" +ex.getStackTrace().toString();
                LOGGER.log(Level.WARNING, msg);
            } catch (IOException ex) {               
                
                LOGGER.log(Level.SEVERE,ex.getMessage(),ex);
            } finally {
                try {
                    out.close();
                } catch (IOException ex) {
                    //System.out.println(ex.getMessage());
                    String msg = ex.toString() +"--" +ex.getStackTrace().toString();
                    LOGGER.log(Level.WARNING, msg);
                }
            }

        }

    }

    public boolean alyaIsRunning(String pid) {

        boolean running = false;

        try {

            System.out.println("checking alya running");
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

        }

        return running;
    }
    
    private void deleteFiles(){
        
        File dir = new File(config.getAlyaRepositoryLocal());
        
            for(File file: dir.listFiles()) {
                if( file.length() < 1){
                    file.delete();
                }
            }
    
    }

}
