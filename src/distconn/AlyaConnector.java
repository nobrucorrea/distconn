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
public class AlyaConnector extends Thread {

    Config config;
    SCPClient scpADAN;
    SCPClient scpAlya;
    Connection conADAN;
    Connection conAlya;
    File key;
    

    public AlyaConnector(Config config) throws IOException {

        
        this.config = config;

        conAlya = new Connection(this.config.getAlyaHost());

        conAlya.connect();

        key = new File(config.getPrivatekey());

        boolean isAuthenticated = conAlya.authenticateWithPublicKey(config.getAdanUserHost(), key, null);

        if (isAuthenticated) {

            scpAlya = conAlya.createSCPClient();
            System.out.println("ALya connected ....");

        } else {

            System.out.println("login Alya failure");

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
                scpAlya.get(config.getAlyaFile(), out);
                out.close();

                //carregando o repositorio com os arquivos baixados do servidor 
                File alyaDirectory = new File(config.getAlyaRepositoryLocal());

                //testa para ver se ja existe algum arquivo no repositorio
                //caso nao exista nao é preciso comparar apenas enviar para o adan
                if (alyaDirectory.listFiles().length > 2) {
                    //comparando os arquivos                    
                    List<File> alyaFiles = ConnectorUtils.getLastFiles(config.getAlyaRepositoryLocal());

                    if (!ConnectorUtils.compareFiles(alyaFiles.get(0), alyaFiles.get(1))) {

                        //Enviar arquivo para adan
                        conADAN = new Connection(config.getAdanHost());
                        conADAN.connect();
                        boolean isADANAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);

                        if (isADANAuthenticated) {

                            scpADAN = conADAN.createSCPClient();
                            scpADAN.put(alyaFiles.get(0).getAbsolutePath(), config.getAdanInputDiretory());

                            //apagar arquivo alya
                            ConnectorUtils.removeRemoteFile(conAlya, config.getAdanFile());
                        }

                    }

                } else {
                    
                    System.out.println("sending first file to adan");
                    Thread.sleep(50000);
                    File file = new File(config.getAlyaRepositoryLocal());

                    conADAN = new Connection(config.getAdanHost());
                    conADAN.connect();
                    boolean isAlyaAuthenticated = conADAN.authenticateWithPublicKey(config.getAdanUserHost(), key, null);

                    if (isAlyaAuthenticated) {
                        scpADAN = conADAN.createSCPClient();
                        File f = file.listFiles()[0];
                        scpADAN.put(f.getAbsolutePath(), config.getAdanInputDiretory());
                        System.out.println("File sent to ADAN");
                    } else {
                        System.out.println("ADAN authenticated failed");
                    }

                    

                }

                Thread.sleep(config.getAlyaThreadSleep());

            } catch (FileNotFoundException ex) {
                Logger.getLogger(ADANConnector.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(ADANConnector.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(ADANConnector.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    out.close();
                } catch (IOException ex) {
                    Logger.getLogger(ADANConnector.class.getName()).log(Level.SEVERE, null, ex);
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
