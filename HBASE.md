# HBASE_COMMANDE
 
 ### Créer un namespace si vous ete admin sinon on peux pas

* create_namespace 'ton_namespace'

### Efacer un namespace si il est vide sinon on peux pas 

* drop_namespace 'ton_namespace'

### Liste des namespace 'Admin' si vous ete admin

* list_namespace

### Liste des tables présentes dans un namespace

* list_namespace_tables 'ton_namespace'

### Modifier les attributs d'un namespace

* alter_namespace 'ton_namespace', {METHOD=>'set', 'PROPERTY_NAME'=>'PROPERTY_VALUE'}

### Lire les attributs d'un namespace

* describe_namespace 'ton_namespace'

### create : créer une table

* create '<table_name>','<colfam1>', '<colfam2>', ...., '<colfamN>'

### insertion des données dans une table 

* put '<ton_namespace:table_name>','row','<colfam2:column>','<value>'

### lire une valeur d'une ligne 

* get '<ton_namespace:table_name>','row'

### Lister les tables dans HBase.

* list 'ton_namespace:ta_table'

### désactiver/activer une table. Désactivation obligatoire avant suppression
 * is_disabled 'ton_namespace:ta_table' et is_enabled 'ton_namespace:ta_table'

### Afcher les attributs d'une table

* describe 'ton_namespace:ta_table' 

### Afcher tout le contenu d'une table

* scan 'ton_namespace:ta_table'

### efacer une table de Hbase. la table doit être désactivée.

* drop 'ton_namespace:ta_table'

### tester si une table existe
* exists 'ton_namespace:ta_table'

### affiche le nombre de ligne dans la table
* count 'ton_namespace:ta_table'

### désactive + efface + recrée la table
* truncate 'ton_namespace:ta_table'

### interdire l'écriture sur une table


### effacer une columnFamily

* alter 'ton_namespace:ta_table','delete'=> 'cf2'

### Ecrire/mettre à jour une donnée
* put 'ton_namespace:ta_table','<rowid>','<cf:colname>','<value>'
* put 'datalab:matable_test','1','cf1:age','999'

### Lire une ligne d'une table
* get 'ton_namespace:ta_table', '<rowid>'

### Effacer une cellule 
* delete '<tabname>', '<row>', '<colname>', '<timestamp>'

#
### pour charger un fichier csv en Hbase

1)envoyé le fichier sur le réseau avec " winScp " et puis tu l'envoie sur HDFS

'il faut charger le fichier en HDFS'

reseau:$ 'cette comande: hadoop fs -copyFromLocal emp_data.csv /user/a4175704/les_fichier_csv'

'pour le importer sur Hbase'

* hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=',' -  
  Dimporttsv.columns='HBASE_ROW_KEY,cf1:ename,cf1:designation,cf1:manager,cf2:hire_date,cf2:sal,cf2:deptno' datalab:emp_data    
   /user/a4175704/les_fichier_csv/emp_data.csv


### pour visualiser un fichier dans hadoop 

* 'hadoop fs -cat /user/a4175704/les_fichier_csv/emp_data.csv'

### Quelque TP HBASE
### https://insatunisia.github.io/TP-BigData/tp4/
