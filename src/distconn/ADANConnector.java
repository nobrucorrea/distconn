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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Bruno
 */
public class ADANConnector extends Thread {

    Config config;
    SCPClient scpADAN;
    SCPClient scpAlya;
    Connection conADAN;
    Connection conAlya;
    File key;
    

    public ADANConnector(Config config) throws IOException {

        
        this.config = config;

        conADAN = new Connection(this.config.getAdanHost());

        conADAN.connect();

        key = new File(config.getPrivatekey());

        boolean isAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);

        if (isAuthenticated) {

            scpADAN = conADAN.createSCPClient();
            System.out.println("ADAN connected ....");

        } else {

            System.out.println("login ADAN failure");

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
                scpADAN.get(config.getAdanFile(), out);
                System.out.println("download adan file");
                out.close();

                Thread.sleep(5000);
                //carregando o repositorio com os arquivos baixados do servidor 
                System.out.println(config.getAdanRepositoryLocal());
                File adanDirectory = new File(config.getAdanRepositoryLocal());

                //testa para ver se ja existe algum arquivo no repositorio
                //caso nao exista nao é preciso comparar apenas enviar para o alya
                if (adanDirectory.listFiles().length > 2) {
                    //comparando os arquivos                    
                    List<File> adanFiles = ConnectorUtils.getLastFiles(config.getAdanRepositoryLocal());

                    if (!ConnectorUtils.compareFiles(adanFiles.get(0), adanFiles.get(1))) {
                        System.out.println("Arquivos diferentes");
                        //Enviar arquivo para alya
                        conAlya = new Connection(config.getAlyaHost());
                        conAlya.connect();
                        boolean isAlyaAuthenticated = conAlya.authenticateWithPublicKey(config.getAlyaUserHost(), key, null);

                        if (isAlyaAuthenticated) {

                            scpAlya = conAlya.createSCPClient();
                            scpAlya.put(adanFiles.get(0).getAbsolutePath(), config.getAlyaInputDiretory());

                            //apagar arquivo adan
                            //ConnectorUtils.removeRemoteFile(conADAN, config.getAdanFile());
                        }

                    }else{
                        System.out.println("Rquivos iguais, nao envio");
                    }

                } else {

                    System.out.println("sending first file to alya");
                    Thread.sleep(50000);
                    File file = new File(config.getAdanRepositoryLocal());

                    conAlya = new Connection(config.getAlyaHost());
                    conAlya.connect();
                    boolean isAlyaAuthenticated = conAlya.authenticateWithPublicKey(config.getAlyaUserHost(), key, null);

                    if (isAlyaAuthenticated) {
                        scpAlya = conAlya.createSCPClient();
                        File f = file.listFiles()[0];
                        scpAlya.put(f.getAbsolutePath(), config.getAlyaInputDiretory());
                        System.out.println("File sent to Alya");
                    } else {
                        System.out.println("Alya authenticated failed");
                    }

                }

                System.out.println("Sleeping...");
                Thread.sleep(config.getAdanThreadSleep());

            } catch (FileNotFoundException ex) {
                System.out.println(ex.getMessage());
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            } finally {
                try {
                    out.close();
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }

        }

       
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
                    
                }else{
                    
                    if(line.contains(pid)){
                        running = true;
                    }
                }

            }
        } catch (IOException e) {

        }

        return running;
    }



}
