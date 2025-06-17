package org.isihop.fr.jrename_fcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tondeur-h
 */
public class JRename_FCS {

    String dburl = "";
    String dblogin = "";
    String dbpassword = "";
    String srcPath = "";
    String dstPath = "";
    String extension = "";

    List<String> listFichier = new ArrayList<>();

    //database
    Connection conn;
    Statement stmt;
    
    //logs
    private static final Logger logger = Logger.getLogger(JRename_FCS.class.getName());

    public static void main(String[] args) {
        new JRename_FCS();
    }

    public JRename_FCS() {
        //lire properties
        lire_properties();
        //lister les fichiers FCS du dossier source
        lister_fichiers();
        //traiter la liste des fichiers
        traiter_fichiers();
    }

    /**
     * *******************************
     * Lire les properties du fichier properties local.
     ********************************
     */
    private void lire_properties() {
        String currentPath = System.getProperty("user.dir");
        String programName = this.getClass().getSimpleName();
        FileInputStream is = null;

        try {
            is = new FileInputStream(currentPath + "/" + programName + ".properties");
            Properties p = new Properties();
            try {
                //charger le fichier properties
                p.load(is);
                //lecture des variables
                dburl = p.getProperty("dburl", "jdbc:postgresql://vm296.ch-v.net:5432/cmf");
                dblogin = p.getProperty("dblogin", "postgres");
                dbpassword = p.getProperty("dbpassword", "password");
                srcPath = p.getProperty("srcpath", "e:/echanges/cmf/");
                dstPath= p.getProperty("dstpath", "e:/echanges/cmf/kaluza");
                extension = p.getProperty("extension", "fcs");
            } catch (IOException ex) {
                logger.log(Level.SEVERE, ex.getMessage());
            }
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                logger.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    /**
     * ****************************
     * Lister tous les fichiers CSV du dossier local fichiers
     *****************************
     */
    private void lister_fichiers() {
        File[] filesInDirectory = new File(srcPath).listFiles();
        for (File f : filesInDirectory) {
            String filePath = f.getAbsolutePath();
            String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
            if (extension.equals(fileExtenstion)) {
                listFichier.add(filePath);
            }
        }
    }

    /********************************
     * Renommer les fichiers
     ********************************/
    private void traiter_fichiers() {
        String numechantillon="";
        String nouveauNom="";
        connect_db(); //connecter DB
        //pour :chaque fichier
        for (String fichier:listFichier)
        {
        //lire le nom et récupérer le numero d'échantillon
        numechantillon=extraire_numechantillon(fichier);
        //rechercher en base les données correspondantes
        nouveauNom=recherher_donnee(numechantillon.trim());
        //renommer le fichier
        renommer_et_deplacer_fichier(fichier,nouveauNom,dstPath);
        } //fin pour
        close_db(); //fermer db
    }
    
    
    /****************************
     * Connecter la DB
     * Si non possible pas de traitement
     * @return boolean
     ****************************/
    private boolean connect_db() {
        boolean isconnected=false;
        try {
            Class.forName("org.postgresql.Driver");
       
                conn = DriverManager.getConnection(dburl,dblogin,dbpassword);
                isconnected=true;
                stmt=conn.createStatement();
                
        } catch (ClassNotFoundException | SQLException ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
    return isconnected;  
    }

    /*********************************
     * Fermer la database si possible.
     *********************************/
    private void close_db() {
        try {
            stmt.close();
            conn.close();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, ex.getMessage());
        }
    }

    /*******************************
     * Verifier présence ligne en DB
     * @param laLigne
     * @return 
     *******************************/
    private String recherher_donnee(String numechantillon) {
        String retour="";
        try {
            String sql="SELECT echantillon,object,nature,panel FROM public.patients WHERE echantillon='"+numechantillon+"'";
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                retour=rs.getString(1)+"_"+rs.getString(2)+"_"+rs.getString(3)+"_"+rs.getString(4);
            }
            
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "NUM ligne : {0}   {1}", new Object[]{numechantillon, ex.getMessage()});
        }
        
        return retour;
    }

    /*****************************
     * Extraction du numéro d'échantillon
     * @param fichier
     * @return 
     ********************************/
    private String extraire_numechantillon(String fichier) {
        String numEchantillon="";
        
        int posPoint=fichier.lastIndexOf(".");
         numEchantillon = numEchantillon.substring(0, posPoint);
        return numEchantillon;
    }

    
    /*********************************
     * Renommer le fichier avec le nouveau nom
     * @param fichier
     * @param nouveauNom 
     *********************************/
    private boolean renommer_et_deplacer_fichier(String fichiersource, String nouveauNom, String destination) {
        File fichierSource = new File(fichiersource);
        File dossierDest = new File(destination);

        // Vérifie si le fichier source existe
       if (!fichierSource.exists()) {
        System.out.println("Le fichier source n'existe pas.");
        return false;
        }

        // Crée le dossier de destination s'il n'existe pas
        if (!dossierDest.exists()) {dossierDest.mkdirs();}

        // Crée le chemin complet du nouveau fichier
        File fichierDestination = new File(dossierDest, nouveauNom+"."+extension);

        try {
            Files.move(fichierSource.toPath(), fichierDestination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Fichier déplacé et renommé avec succès !");
            return true;
        } catch (IOException e) {
            System.out.println("Erreur lors du déplacement : " + e.getMessage());
            return false;
        }  
    }

}
