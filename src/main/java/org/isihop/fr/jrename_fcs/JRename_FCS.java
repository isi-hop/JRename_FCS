package org.isihop.fr.jrename_fcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author tondeur-h
 */
public class JRename_FCS {

    String dburl = "";
    String dblogin = "";
    String dbpassword = "";
    List<String> srcPathList;
    String srcPathStrList="";
    String dstPath = "";
    String extension = "";
    String action="";
    boolean supression=false;

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
        srcPathList=new ArrayList<>(Arrays.asList(srcPathStrList.split(",")));
        
        connect_db(); //connecter DB 
        lister_fichiers();
        close_db(); //fermer db

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
                srcPathStrList = p.getProperty("srcpathlist", "e:/echanges/cmf/");
                dstPath= p.getProperty("dstpath", "e:/echanges/cmf/kaluza");
                extension = p.getProperty("extension", "fcs");
                action=p.getProperty("action","move");
                supression=Boolean.parseBoolean(p.getProperty("supression", "false"));
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
    private void lister_fichiers() 
    {
        for (String srcPath:srcPathList)
        {
            File[] directoriesInDirectory = new File(srcPath).listFiles(); //lister tous les dossiers
            for (File d : directoriesInDirectory) //pour chaque dossier du dossier source
            {
                if (d.isDirectory())
                {
                    String dirPath = d.getAbsolutePath();
                    File[] filesInDirectory = new File(dirPath).listFiles();
                    for (File f : filesInDirectory) //pour chaque ficihier du sous dossier
                    {
                        String filePath = f.getAbsolutePath();
                        String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
                        if (extension.equals(fileExtenstion)) {traiter_fichiers(filePath);}
                    }
                } //fin traitement sous dossier
                //on peut supprimer si nécessaire la source ici
                if (supression) 
                {
                    try {
                        supprimerRecursivement(d.toPath());
                    } catch (IOException ex) 
                    {
                        Logger.getLogger(JRename_FCS.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
    }

    /********************************
     * Renommer les fichiers
     ********************************/
    private void traiter_fichiers(String fichier) 
    {
        String numechantillon="";
        String nouveauNom="";
        //lire le nom et récupérer le numero d'échantillon
        String fichier_reduit=new File(fichier).getName().trim();
        numechantillon=extraire_numechantillon(fichier_reduit);
        //rechercher en base les données correspondantes
        nouveauNom=rechercher_donnees(numechantillon,fichier_reduit);
        //renommer le fichier
        renommer_et_deplacer_fichier(fichier,nouveauNom,dstPath);
    }
    
    
    /****************************
     * Connecter la DB
     * Si non possible pas de traitement
     * @return boolean
     ****************************/
    private boolean connect_db() 
    {
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
    private void close_db() 
    {
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
    private String rechercher_donnees(String numechantillon,String nom_fichier) 
    {
        String retour=nom_fichier.substring(0, nom_fichier.lastIndexOf("."));
        try {
            String sql="SELECT object,panel FROM public.patients WHERE echantillon='"+numechantillon+"'";
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                retour=retour+"_"+rs.getString(1)+"_"+rs.getString(2);
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
    private String extraire_numechantillon(String fichier) 
    {
        String numEchantillon="";
        //Format d'echantillon fcs => [251672590001][SCREEN SLP]SCREEN SLP DF1 20250618 1118.fcs
        //rechercher le premier [ + 1 et le premier ]
        //extraite le nom seul avec extension        
        Pattern pt=Pattern.compile("\\b\\d{12}\\b");
        Matcher m=pt.matcher(fichier);

        if (m.find()) {
        numEchantillon = fichier.substring(m.start(), m.end());
        }
        
        return numEchantillon;
    }

    
    /*********************************
     * Renommer le fichier avec le nouveau nom
     * @param fichier
     * @param nouveauNom 
     *********************************/
    private boolean renommer_et_deplacer_fichier(String fichiersource, String nouveauNom, String destination) 
    {
        destination=destination+"/"+LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));        
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
            if (action.compareToIgnoreCase("copy")==0) {Files.copy(fichierSource.toPath(), fichierDestination.toPath(), StandardCopyOption.REPLACE_EXISTING);}
            if (action.compareToIgnoreCase("move")==0) {Files.move(fichierSource.toPath(), fichierDestination.toPath(), StandardCopyOption.REPLACE_EXISTING);}
            System.out.println("Fichier déplacé et renommé avec succès !");
            return true;
        } catch (IOException e) {
            System.out.println("Erreur lors du déplacement : " + e.getMessage());
            return false;
        }  
    }
    
    
    /****************************************
     * Supprimer récursivement les fichiers et dossiers
     * @param chemin
     * @throws IOException 
     ****************************************/
    public static void supprimerRecursivement(Path chemin) throws IOException 
    {
        Files.walkFileTree(chemin, new SimpleFileVisitor<Path>() 
        {
            @Override
            public FileVisitResult visitFile(Path fichier, BasicFileAttributes attrs) throws IOException {
            Files.delete(fichier);
            return FileVisitResult.CONTINUE;
        }

            @Override
            public FileVisitResult postVisitDirectory(Path dossier, IOException exc) throws IOException 
            {
                Files.delete(dossier);
                return FileVisitResult.CONTINUE;
            }
        });
    }


}
