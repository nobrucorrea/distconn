/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distconn;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
        
        if(isAuthenticated){
        
           scpADAN = conADAN.createSCPClient();
           System.out.println("ADAN connected ....");          
                      
        }else{
        
            System.out.println("login ADAN failure");
    
        }
    
    }
    
    
    @Override
    public void run(){
    
        
        while (adanIsRunning()) {
            
            FileOutputStream out = null;
            try {
                
                
                //pegando arquivo na maquina do alya e
                //salvando com o nome do horario do download
                String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
                out = new FileOutputStream(config.getAdanRepositoryLocal()+ timeStamp);                          
                scpADAN.get(config.getAlyaFile(), out);
                out.close();
                
                //carregando o repositorio com os arquivos baixados do servidor 
                File adanDirectory = new File(config.getAdanRepositoryLocal());
                
                //testa para ver se ja existe algum arquivo no repositorio
                //caso nao exista nao é preciso comparar apenas enviar para o adan
                if (adanDirectory.listFiles().length > 2) {
                    //comparando os arquivos                    
                    List<File> adanFiles = ConnectorUtils.getLastFiles(config.getAdanRepositoryLocal());          
                    
                    
                    if( !ConnectorUtils.compareFiles(adanFiles.get(0), adanFiles.get(1)) ){                    
                        
                        //Enviar arquivo para alya
                         conAlya =  new Connection(config.getAlyaHost());
                         conAlya.connect();
                         boolean isAlyaAuthenticated = conAlya.authenticateWithPublicKey(config.getAlyaUserHost(), key, null);
                         
                        if(isAlyaAuthenticated){
                            
                            scpAlya = conAlya.createSCPClient();
                            scpAlya.put(adanFiles.get(0).getAbsolutePath(), config.getAlyaInputDiretory());
                            
                             //apagar arquivo adan
                            ConnectorUtils.removeRemoteFile(conADAN, config.getAdanFile());
                        }
                       
                        
                    
                    }
                    
                    
                    
                    
                }else{
                    
                    List<File> adanFiles = ConnectorUtils.getLastFiles(config.getAdanRepositoryLocal());
                    scpAlya.put(adanFiles.get(0).getAbsolutePath(),  config.getAlyaInputDiretory());
                    
                }
                
                Thread.sleep(config.getAdanThreadSleep());
                
                
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
    
    
    
    public boolean adanIsRunning(){
    
        
        //verificar se o alya ainda esta rodando
        //repciso saber o noem do execultavel
        
        return true;
    }
    
}
