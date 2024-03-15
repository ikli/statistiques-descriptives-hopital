#!/usr/bin/env python
# coding: utf-8


#charger les bibliothéques
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date
import sys
import os


#fichier qui contient les variables à paramétrer pour générer le rapport
separator = "="
variables = {}

with open('variables','r') as f:

    for line in f:
        if separator in line:            
            variable, valeur = line.split(separator, 1)            
            # strip() removes white space from the ends of strings
            variables[variable.strip()] = valeur.strip()



#le chemin pour les quatres fichiers CSV
if len(sys.argv) >=2 : # cette ligne ne fonctionne pas sur jupyter juste en fichier python
    path = sys.argv[1] 
else:    
    path = variables['path']

#le client :
client =  variables['client']

#Unité négligeables:
 
 #nbr_rum_moins de :
negl_nbr_rum = int(variables['negl_nbr_rum'])

#les codes dont apparaition est moins de n fois
dp_max =  int(variables['dp_max'])

#les ccam dont apparaition est moins de (seuil_acte_rare) fois

seuil_acte_rare = int(variables['seuil_acte_rare'])

#le nombre de diagnostics principaux frequents à afficher
plot_dp =  int(variables['plot_dp'] )

#le nombre de diagnostics reliés frequents à afficher
plot_dr =  int(variables['plot_dr'] )

#le nombre de diagnostics associés frequents à afficher
plot_das =  int(variables['plot_das'] )

#le nombre d'actes frequents à afficher
plot_acte =  int(variables['plot_acte'] )

#le nombre de couple dp-dr à afficher qui viennent ensemble
n_dp_dr =  int(variables['n_dp_dr'] )


#création de répertoires
if not os.path.exists('rapports/u_medicales'):
    os.makedirs('rapports/u_medicales')
    

#importer les données:
pmsi_rum = pd.read_csv(path +  "/pmsi_rum.csv", delimiter= ';',  decimal="," )

#données de diagnistics
pmsi_diag = pd.read_csv(path + "/pmsi_diag.csv", delimiter= ';', decimal="," )

#données des actes
pmsi_act = pd.read_csv(path +  "/pmsi_act.csv", delimiter= ';',decimal="," )

#préparation et Nettoyage

#mettre les UMs en format STR
pmsi_rum['pmr_um'] = pmsi_rum['pmr_um'].apply(str)

# supprimer les lignes diagnostics dont le code est manquant
pmsi_diag = pmsi_diag.dropna(subset=['prd_code'])

#mettres les types codes des diags en miniscule
pmsi_diag['prd_type'].apply(lambda x : x.lower())

#garder juste la premiére lettre de type code 
pmsi_diag["prd_type"] = pmsi_diag["prd_type"].str.slice(start=0, stop=1)

# récuperer les identifiants de rum avec un dp 
pmsi_diag_dp = pmsi_diag[pmsi_diag['prd_type'] == "p"] 
pmsi_diag_dp_serie = pmsi_diag_dp['pmsi_rum_pmr_id']


#selectionner dans les dataframes rum, diag, ccam les lignes avec ces identifiants

pmsi_rum = pmsi_rum[pmsi_rum['pmr_id'].isin(pmsi_diag_dp_serie)]
pmsi_rum.index = range(pmsi_rum.shape[0])

pmsi_diag = pmsi_diag[pmsi_diag['pmsi_rum_pmr_id'].isin(pmsi_diag_dp_serie)]
pmsi_diag.index = range(pmsi_diag.shape[0])

pmsi_act = pmsi_act[pmsi_act['pmsi_rum_pmr_id'].isin(pmsi_diag_dp_serie)]
pmsi_act.index = range(pmsi_act.shape[0])


#gestion des dates 
pmsi_rum["pmr_startdate"] = pd.to_datetime(pmsi_rum["pmr_startdate"],dayfirst = True)
pmsi_rum["pmr_enddate"] = pd.to_datetime(pmsi_rum["pmr_enddate"],dayfirst = True)

#la date début de la période des statistiques

date_debut  =  variables['date_debut'] 
if date_debut != "all":    
    date_debut = pd.to_datetime(date_debut, infer_datetime_format=True,dayfirst = True)     
    date_debut = max(date_debut,pmsi_rum['pmr_startdate'].min())
    #recharger les csv en filtrant selon la plage de dates demandée
    pmsi_rum = pmsi_rum[pmsi_rum['pmr_startdate'] >= date_debut] 
    pmsi_rums_serie = pmsi_rum['pmr_id']   
  
    pmsi_diag = pmsi_diag[pmsi_diag['pmsi_rum_pmr_id'].isin(pmsi_rums_serie)]
    pmsi_diag.index = range(pmsi_diag.shape[0])

    pmsi_act = pmsi_act[pmsi_act['pmsi_rum_pmr_id'].isin(pmsi_rums_serie)]
    pmsi_act.index = range(pmsi_act.shape[0])
    
#la date de fin de la période à réaliser les statistiques
 
date_fin =  variables['date_fin'] 

if date_fin != "all":
    date_fin = pd.to_datetime(date_fin, infer_datetime_format=True)   
    date_fin = min(date_fin, pmsi_rum['pmr_enddate'].max())
    #recharger les csv en filtrant selon la plage de dates demandée
    pmsi_rum = pmsi_rum[pmsi_rum['pmr_enddate'] <= date_fin] 
    pmsi_rums_serie = pmsi_rum['pmr_id']   
  
    pmsi_diag = pmsi_diag[pmsi_diag['pmsi_rum_pmr_id'].isin(pmsi_rums_serie)]
    pmsi_diag.index = range(pmsi_diag.shape[0])

    pmsi_act = pmsi_act[pmsi_act['pmsi_rum_pmr_id'].isin(pmsi_rums_serie)]
    pmsi_act.index = range(pmsi_act.shape[0])
       

#les jointures
#rum et diag
pmsi_rum_diag = pd.merge(pmsi_rum, pmsi_diag, left_on = "pmr_id", right_on = "pmsi_rum_pmr_id", how = "left")

#rum et act
pmsi_rum_act = pd.merge(pmsi_rum, pmsi_act, left_on = "pmr_id", right_on = "pmsi_rum_pmr_id", how = "left")


#Les fonctions renvoyant les statistiques descriptives
def get_rum_total(um_i):
    """
    Fonction qui renvoie le total de RUM dans une unité médicale
    :@param um_i : (str)
    :@return: (int) total des RUM 
  
    """
    return len(pmsi_rum[pmsi_rum['pmr_um'] == um_i])

def get_median_duree(um_i):
    """
    Fonction qui renvoie la mediane de duree des résumés médicales d'une unité médicale
    :@param um_i : (str)
    :@return: (float) mediane de durée des RUMs de l'unité
    """
    df = pmsi_rum[pmsi_rum['pmr_um'] == um_i]
    return df['pmr_duration'].median()
    

def get_dp_tot_distinct(um_i):
    """
    Fonction qui renvoie le nombre des différents diagnostics principaux sans doublons
    :@param um_i : (str)
    :@return: (int) total des DP distinct
    
    """
    dp_count = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "p") & (pmsi_rum_diag.pmr_um == um_i)]  
    return len(set(dp_count.prd_code))
   
def get_dp_frequent(um_i):
    """
    Fonction qui renvoie Le code de diagnostic principal le plus fréquent d'une unité médicale
    :@param um_i : (str)
    :@return: (str) le code de DP le plus fréquent  
    
    """
    dp_code = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "p") & (pmsi_rum_diag.pmr_um == um_i)]  
    return dp_code.groupby('prd_code')['prd_id'].count().idxmax()


def get_dp_frequent_count(um_i):
    """
    Fonction qui renvoie Le nombre de fois de diagnostic principal le plus fréquent d'une unité médicale
    :@param um_i : (str)
    :@return: (int) le nombre de fois d'apparition du code DP le plus fréquent dans une unité médicale
      
    """
    dp_code_count = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "p") & (pmsi_rum_diag.pmr_um == um_i)]  

    return dp_code_count.groupby('prd_code')['pmr_id'].count().max()

def frequence_dp_freq(um_i) :
    """
    Fonction qui renvoie la frequence du dp frequent
    :@param um_i : (str) l'unité médicale
    :@return: (float) la frequence du dp frequent
    """
    return ((get_dp_frequent_count(um_i) / get_rum_total(um_i)) * 100).round(1)


def get_dp_rare_count(um_i):
    """
    Fonction qui renvoie Le nombre de fois de diagnostic principal le moins fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le nombre de fois d'apparition du code DP le moins fréquent dans une unité médicale
  
    """
    df = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'p')]    
    return df.groupby('prd_code')['pmr_id'].count().min()

def frequence_dp_rare(um_i, dp_max= dp_max) :
    """
    Fonction qui renvoie la frequence de codes principaux rares selon un seuil
    :@param umi :(int or str) l'unité médicale
    :@dp_max : (int) le nombre de fois maximal d'apparition de code
    :@return : (float) frequence somme codes rares selon seuil
    """
    df = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'p')]
    df = pd.DataFrame(df.groupby('prd_code')['pmr_id'].count())
    df = df[df['pmr_id'] <= dp_max]
    return round(df.shape[0] * 100 / get_rum_total(um_i), 1)

######diagnostic RELIE##############
def get_dr_total(um_i):
    """
    Fonction qui renvoie le nombre total de diagnostics reliés dans une unité médicale
    :@param um_i : ( str)
    :@return: (int) total des DR
        
    """
    df_dr = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'r')]
    if not df_dr.shape[0]:
        return 0
    else:             
        
        return df_dr.shape[0]
    
def get_dr_tot_distinct(um_i):
    """
    Fonction qui renvoie le nombre total des différents diagnostics reliés
    :@param um_i : ( str)
    :@return: (int) total des DR sans doublons 
    
    """
    df_dr = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'r')]
    if not df_dr.shape[0]:
        return 0
    else:           
        return len(set(df_dr.prd_code))
            
    
def get_dr_frequent(um_i):
    """
    Fonction qui renvoie Le diagnostic relié le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (str) le code du DR le plus fréquent
    
    """
    df_dr = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'r')]
    if not df_dr.shape[0]:
        return "Aucun DR"
    else:             
        
        return  df_dr.groupby('prd_code')['prd_code'].count().idxmax()

def get_dr_frequent_count(um_i):
    """
    Fonction qui renvoie Le nombre de fois de diagnostic relié le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le nombre de fois d'apparition du code DR le plus fréquent dans une unité médicale
        
    """
    df_dr =  pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'r')]
    if not df_dr.shape[0]:
        return 0
    else:             
        
        return df_dr.groupby('prd_code')['prd_code'].count().max()
    
def frequence_dr_frequent(um_i):
    """
    Fonction qui renvoie la frequence du dr fréquent
    :@param um_i : ( str) l'unité médicale
    """
    df_dr = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'r')]
    if not df_dr.shape[0]:
        return 0
    else:
        return round((get_dr_frequent_count(um_i) * 100/  get_rum_total(um_i)) ,1)     
     

##############diagnostic ASSOCIE###############################


def get_das_total(um_i):
    """
    Fonction qui renvoie le nombre de rum avec au moins un das dans une unité médicale
    :@param um_i : ( str)
    :@return: (int) total des rums avec au moins un DAS
    
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'a')]
    if not df_das.shape[0]:
        return 0
    else:       
       
        return len(df_das.groupby('pmr_id'))
def get_das_tot_distinct(um_i):
    """
    Fonction qui renvoie le nombre total des différents diagnostics associés
    :@param um_i : ( str)
    :@return: (int) total des codes Das sans doublons dans une unité médicale
   
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'a')]
    if not df_das.shape[0]:
        return 0
    else:    
        return len(set(df_das.prd_code))
    
    
def get_das_frequent(um_i):
    """
    Fonction qui renvoie Le diagnostic associé le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le code du DAS le plus fréquent
   
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'a')]
    if not df_das.shape[0]:
        return "Aucun DAS"
    else:
        return df_das.groupby('prd_code')['prd_id'].count().idxmax()

def get_das_frequent_count(um_i):
    """
    Fonction qui renvoie Le nombre de fois de diagnostic associé le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le nombre de fois d'aparition du code DR le plus fréquent dans une unité médicale
    
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'a')]
    if not df_das.shape[0]:
        return 0
    else:
        return df_das.groupby('prd_code')['pmr_id'].count().max()
    
def frequence_das_frequent(um_i):
    """
     Fonction qui renvoie la frequence du das fréquent
    :@param um_i : ( str) l'unité médicale
    :return: (float) pourcentage du das frequent 
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag['pmr_um'] == um_i) & (pmsi_rum_diag['prd_type'] == 'a')]
    if not df_das.shape[0]:
        return 0
    else:
        return round((get_das_frequent_count(um_i) * 100/  get_rum_total(um_i)), 1)    



################## les actes #########################""
def get_rum_with_actes(um_i):
    """
    Fonction qui renvoie le nombre des RUM avec au moins un acte dans une unité médicale
    :@param um_i : ( str)
    :@return: (int) total de rums avec acte
        
    """       
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return 0
    else:       
       
        return len(df_actes.groupby('pmsi_rum_pmr_id')['pmr_id'])


def get_act_tot_distinct(um_i):
    """
    Fonction qui renvoie le nombre total des différents actes sans doublons
    :@param um_i : ( str)
    :@return: (int) total des actes sans doublons dans une unité médicale
       
    """
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return 0
    else:     
        return len(set(df_actes.pra_ccam))   
    
def get_acte_frequent(um_i):
    """
    Fonction qui renvoie L'acte le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le code de l'acte le plus fréquent
    
    """
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return "Aucun Acte"
    else:     
        return df_actes.groupby('pra_ccam')['pra_id'].count().idxmax()
    
def get_act_frequent_count(um_i):
    """
    Fonction qui renvoie Le nombre de fois d'acte le plus fréquent d'une unité médicale
    :@param um_i : ( str)
    :@return: (int) le nombre de fois d'apparition du code ccam le plus fréquent dans une unité médicale
    CU : le fichier pmsi_rum_act deja référencié
    
    """            
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return 0
    else:      
        return df_actes.groupby('pra_ccam')['pra_id'].count().max()
    

def frequence_act_frequent(um_i):
    """
     Fonction qui renvoie la frequence de l'acte le plus fréquent
    :@param um_i : ( str) l'unité médicale
    """
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return 0
    else:      
        return round((get_act_frequent_count(um_i) * 100/   get_rum_total(um_i) ), 1)
    

    
def frequence_actes_rare(um_i, seuil_acte_rare= seuil_acte_rare) :
    """
    Fonction qui renvoie la frequence des actes ccam qui aparaissent selon le seuil renseigné
    :@param umi :(str) l'unité médicale
    :@seuil_acte_rare : (int) le nombre de fois maximal d'apparition de code ccam
    :@return : (float) frequence ccam rares selon le seuil
    """
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return 0
    else:
        
        df = pd.DataFrame(df_actes.groupby('pra_ccam')['pmr_id'].count())
        df = df[df['pmr_id'] <= seuil_acte_rare]
        return round(df.shape[0] * 100 /  get_rum_total(um_i), 1)  
##########################################################

def duration(um_i):
    """
    renvoie le dataframe de statistiques de la durée des RUMs
    """
    duration = pd.DataFrame(pmsi_rum[pmsi_rum['pmr_um'] == um_i]['pmr_duration'].describe())
    duration = duration.transpose()
    duration = duration[['mean', 'min', '25%', '50%', '75%', 'max']]
    duration.columns=['moyenne', 'min', 'Q1', 'Mediane', 'Q3', 'max']
    duration['moyenne'] = round(duration['moyenne'],2)

    duration.index = ['durée Rum']
    return duration

    
    
######les graphiques################""
def plot_most_dp_frequents(um_i, plot_dp=plot_dp) :
    """
    Plot qui affiche les n DP les plus fréquents dans une unité médicale
    :@param um_i : ( str)
    :@param plot_dp : (int) nombre de dp les plus frequents
    """
    dp_count = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "p") & (pmsi_rum_diag.pmr_um == um_i)]   
    nlargest = dp_count.groupby('prd_code')['prd_id'].count().nlargest(plot_dp).to_frame().reset_index()
    nlargest.rename(columns = {'prd_id':'Nombre'},inplace = True) 
    nlargest = nlargest.sort_values(by = ['Nombre'] )
    if nlargest.shape[0] <=2 :
        figsize = (3, 1)
    elif nlargest.shape[0] >2 and nlargest.shape[0] <=4  :
        figsize = (4, 2)
            
    elif nlargest.shape[0] > 4 and nlargest.shape[0] <=10 :
        figsize = (6,4)
            
    elif nlargest.shape[0]> 10 : 
        figsize = (9,6)
    plt.figure()
    fig, ax = plt.subplots(figsize=figsize)

    total = get_rum_total(um_i)
    codes = nlargest['prd_code']
    count = nlargest['Nombre']

    percent = nlargest['Nombre']/total*100

    new_labels = [i+'  {:.2f}%'.format(j) for i, j in zip(codes, percent)]

    plt.barh(codes, count, color = 'orange',  edgecolor= None)
    plt.yticks(range(len(codes)), new_labels)
    plt.tight_layout()

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.axes.get_xaxis().set_visible(False)
    ax.tick_params(axis="y", left=False)
    #plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_dp_frequents.png')
    plt.title("les DP les plus fréquents dans l'UM " + str(um_i))
    plt.xlabel('DP')
    plt.ylabel('fréquence')
    plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_dp_frequents.png', bbox_inches = "tight")
    plt.close('all') 
    
    
    
#les cinq DR les plus fréquents dans l'unité médicale
def plot_most_dr_frequents(um_i, plot_dr=plot_dr) :
    """
    Plot qui affiche les n DR les plus fréquents dans une unité médicale
    :@param um_i : ( str)
    :@param plot_dp : (int) nombre dp les plus frequents souhaité à afficher
    """
    df_dr = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "r") & (pmsi_rum_diag.pmr_um == um_i)]   

    if not df_dr.shape[0]:
        return "Aucun DR dans cette UM"
    else :
        nlargest = df_dr.groupby('prd_code')['prd_id'].count().nlargest(plot_dr).to_frame().reset_index()
        nlargest.rename(columns = {'prd_id':'Nombre'},inplace = True) 
        nlargest = nlargest.sort_values(by = ['Nombre'] )
        if nlargest.shape[0] <=2 :
            figsize = (3, 1)
        elif nlargest.shape[0] >2 and nlargest.shape[0] <=4  :
            figsize = (4, 2)
            
        elif nlargest.shape[0] > 4 and nlargest.shape[0] <=10 :
            figsize = (6,4)
            
        elif nlargest.shape[0]> 10 : 
            figsize = (9,6)
            

        plt.figure()
        fig, ax = plt.subplots(figsize = figsize)

        total = get_rum_total(um_i)
        codes = nlargest['prd_code']
        count = nlargest['Nombre']

        percent = nlargest['Nombre']/total*100

        new_labels = [i+'  {:.2f}%'.format(j) for i, j in zip(codes, percent)]

        plt.barh(codes, count, color = 'green',  edgecolor= None)
        plt.yticks(range(len(codes)), new_labels)
        plt.tight_layout()

        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.axes.get_xaxis().set_visible(False)
        ax.tick_params(axis="y", left=False)          
        plt.title("les DR les plus fréquents dans l'UM" + str(um_i),fontsize= 16)

        plt.xlabel(' DR')
        plt.ylabel('fréquence')    
        plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_dR_frequents.png', bbox_inches = "tight")
        plt.close('all')
                

#les cinq DAS les plus fréquents dans l'unité médicale

def plot_most_das_frequents(um_i, plot_das=plot_das):
    """
    fonction qui renvoie le graphique des n das frequents
    :@param um_i : ( str)
    :@param plot_das : (int) nombre dp les plus frequents souhaité à afficher
    """
    df_das = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "a") & (pmsi_rum_diag.pmr_um == um_i)] 
    if not df_das.shape[0]:
        return "Aucun DAS dans cette UM"
    else:         
        nlargest = df_das.groupby('prd_code')['prd_id'].count().nlargest(plot_das).to_frame().reset_index()
        nlargest.rename(columns = {'prd_id':'Nombre'},inplace = True) 
        nlargest = nlargest.sort_values(by = ['Nombre'] )
        if nlargest.shape[0] <=2 :
            figsize = (3, 1)
        elif nlargest.shape[0] >2 and nlargest.shape[0] <=4  :
            figsize = (4, 2)
            
        elif nlargest.shape[0] > 4 and nlargest.shape[0] <=10 :
            figsize = (6,4)
            
        elif nlargest.shape[0]> 10 : 
            figsize = (9,6)
    
        plt.figure()
        fig, ax = plt.subplots(figsize = figsize)

        total = get_rum_total(um_i)
        codes = nlargest['prd_code']
        count = nlargest['Nombre']
        percent = nlargest['Nombre']/total*100
        new_labels = [i+'  {:.2f}%'.format(j) for i, j in zip(codes, percent)]

        plt.barh(codes, count, color = 'magenta',  edgecolor= None)
        plt.yticks(range(len(codes)), new_labels)
        plt.tight_layout()

        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.axes.get_xaxis().set_visible(False)
        ax.tick_params(axis="y", left=False)          
        plt.title("les DAS les plus fréquents dans l'UM " + str(um_i),fontsize= 16)

        plt.xlabel('DAS')
        plt.ylabel('fréquence')    
        #plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_das_frequents.png')   
        plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_das_frequents.png', bbox_inches = "tight")
        plt.close('all')          
                      
    
#les cinq ACTES les plus fréquents dans l'unité médicale
       
def plot_most_act_frequents(um_i, plot_acte = plot_acte):
    """
    fonction qui renvoie le graphique des n actes fréquents
    :@param um_i : ( str)
    :@param plot_acte : (int) nombre actes les plus frequents souhaité à afficher
    """
    df_actes = pmsi_rum_act[(pmsi_rum_act['pmr_um'] == um_i) & (~pmsi_rum_act['pra_ccam'].isnull())]
    if not df_actes.shape[0]:
        return "Aucun acte dans cette UM"
    else:
        nlargest = df_actes.groupby('pra_ccam')['pmr_id'].count().nlargest(plot_acte).to_frame().reset_index()
        nlargest.rename(columns = {'pmr_id':'Nombre'},inplace = True) 
        nlargest = nlargest.sort_values(by = ['Nombre'] )
        if nlargest.shape[0] <=2 :
            figsize = (3, 1)
        elif nlargest.shape[0] >2 and nlargest.shape[0] <=4  :
            figsize = (4, 2)
            
        elif nlargest.shape[0] > 4 and nlargest.shape[0] <=10 :
            figsize = (6,4)
            
        elif nlargest.shape[0]> 10 : 
            figsize = (9,6)

        plt.figure()
        fig, ax = plt.subplots(figsize = figsize)

        total = get_rum_total(um_i)
        codes = nlargest['pra_ccam']
        count = nlargest['Nombre']
        percent = nlargest['Nombre']/total*100
        new_labels = [i+'  {:.2f}%'.format(j) for i, j in zip(codes, percent)]

        plt.barh(codes, count, color = 'darkorchid',  edgecolor= None)
        plt.yticks(range(len(codes)), new_labels)
        plt.tight_layout()

        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.axes.get_xaxis().set_visible(False)
        ax.tick_params(axis="y", left=False)     
        
        plt.title("Les Actes les plus fréquents dans l'UM " + str(um_i),fontsize= 16)
        plt.xlabel('Les actes médicaux')
        plt.ylabel('fréquence')
        plt.savefig('rapports/u_medicales/'+str(um_i)+'/plots/most_actes_frequents.png', bbox_inches = "tight")
        plt.close('all')         
        
    
########################################################################################
def dp_dr(um_i, n_dp_dr=n_dp_dr ):
    """
    fonction qui renvoie le couple (dp,dr) le plus frequent
    """
    #diagnostic principal
    df_dp = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "p") & (pmsi_rum_diag.pmr_um == um_i)]   
    df_dp = df_dp[["pmsi_rum_pmr_id", "pmr_um","prd_code"]]
    df_dp.columns = ["pmr_id", "um","dp"]

    #diagnostic relié
    df_dr = pmsi_rum_diag[(pmsi_rum_diag.prd_type == "r") & (pmsi_rum_diag.pmr_um == um_i)]   
    df_dr = df_dr[["pmsi_rum_pmr_id", "prd_code"]]
    df_dr.columns = ["pmr_id", "dr"]
       

    if not df_dr.shape[0]:
        return "Aucun DR dans cette UM"
    else:
        data = pd.merge(df_dp, df_dr, how="left", on="pmr_id" )
        res = data.groupby('dp')['dr'].value_counts().nlargest(n_dp_dr )
        res = pd.DataFrame(res)
        res.columns = ['Nombre']
        res = res.assign( pourcentage = res.Nombre.apply(lambda x : round(x * 100/ get_dr_total(um_i) ))) 
        res.index = [res.index[i] for i in range(len(res))] 
        res = res.rename_axis('(DP,DR)').reset_index()
        return res
    
    
def count_seances(um_i):
    """
    Fonction qui renvoie le nombre de rum dédié au séances
    :@param um_i : (str)
    :@return: (int) le nombre de rum qui sont des séances
    """
    pmsi_rum_seances = pmsi_rum[(pmsi_rum["ghm"].str.match('^28.*')== True) & (pmsi_rum['pmr_um'] == um_i)]
    return pmsi_rum_seances .shape[0]


def to_french_date(DatePandas):
    """
    Fonction qui renvoie la date sous forme dd-mm-aaaa de type str
    :@param DatePandas :(timestamps) la date en format date de pandas
    :@return : (str) la date sous format dd-mm-aaaa
    """
    
    date = str(DatePandas).split(" ")[0].split('-')
    date = date[2]+"-"+date[1]+"-"+date[0]
    return date

def activer_lien(val):
    """
    Fonction qui convertit les valeurs du dataframe en lien
    >>> activer_lien(1)
    '<a href="1.html">1</a>'
    """
    return f'<a href="u_medicales/{val}/{val}.html">{val}</a>'
##########################################################################################



df1   = pmsi_rum.groupby(['pmr_um'],as_index = False )[['pmr_startdate']].min()
df2   = pmsi_rum.groupby(['pmr_um'] ,as_index = False)[['pmr_enddate']].max()
df_UM = pd.merge(df1, df2, on ='pmr_um', how = 'outer')
#les unités médicales pour les quelles on souhaite réaliser les statistiques
UM =  variables['UM']  

if UM == "all":
    UM = df_UM
else :
    UM = tuple(map(str, UM.split(','))) 
    UM = df_UM.query('pmr_um in @UM')
df_final = UM.assign(Total_RUMs = df_UM['pmr_um'].apply(lambda x : get_rum_total(x)))
df_final. reindex(columns = ['pmr_um', 'Total_RUMs', 'pmr_startdate','pmr_enddate'])
#mediane de duree de rum 
df_final = df_final.assign(mediane_duree = df_final['pmr_um'].apply(lambda x : get_median_duree(x)))

#part de l'activité
df_final = df_final.assign(part_activite = df_final['pmr_um'].apply(lambda x : round((get_rum_total(x) * 100/ pmsi_rum.shape[0]),1) ))

#nombre de séances:
df_final = df_final.assign(Nombre_seances = df_final['pmr_um'].apply(lambda x : count_seances(x) ))

#pourcentage de séances
df_final = df_final.assign(part_seances = df_final['pmr_um'].apply(lambda x : round((count_seances(x) * 100 / get_rum_total(x)),1) ))


#nombre de Dp distinct
df_final = df_final.assign(dp_distinct_total = df_final['pmr_um'].apply(lambda x : get_dp_tot_distinct(x)))

#le dp le plus frequent

df_final = df_final.assign(dp_frequent = df_final['pmr_um'].apply(lambda x : get_dp_frequent(x)))

# frequence dp frequent

df_final = df_final.assign(frequence = df_final['pmr_um'].apply(lambda x : frequence_dp_freq(x)))
#min d'apparition d'un code dp
#df_final = df_final.assign(Min_dp = df_final['pmr_um'].apply(lambda x : get_dp_rare_count(x)))

# frequence dp rare avec appatition dp_max fois

df_final = df_final.assign(frequence_dp_rare = df_final['pmr_um'].apply(lambda x : frequence_dp_rare(x)))

# nombre de RUM avec un DR

df_final = df_final.assign(nombre_rum_dr = df_final['pmr_um'].apply(lambda x : get_dr_total(x)))

#pourcentage des RUMs qui ont un DR
df_final = df_final.assign(pourcentage_rum_dr = df_final['pmr_um'].apply(lambda x : round(get_dr_total(x) * 100 / get_rum_total(x),1)))


# nombre de DR Distinct par unité médicale
df_final = df_final.assign(dr_total_distinct = df_final['pmr_um'].apply(lambda x : get_dr_tot_distinct(x)))

#le dr le plus fréquent

df_final = df_final.assign(dr_frequent = df_final['pmr_um'].apply(lambda x : get_dr_frequent(x)))

#la frequence du DRs frequents qui apparaissent n fois

df_final = df_final.assign(frequence_dr_freq = df_final['pmr_um'].apply(lambda x : frequence_dr_frequent(x) ))

# nombre des RUM avec au moins un DAS

df_final = df_final.assign(Rums_avec_das = df_final['pmr_um'].apply(lambda x : get_das_total(x) ))

#pourcentage des RUMs qui ont un DAS
df_final = df_final.assign(pourcentage_rum_das = df_final['pmr_um'].apply(lambda x : round(get_das_total(x) * 100 / get_rum_total(x),1)))


# nombre de Das Distinct par unité médicale
df_final = df_final.assign(das_total_distinct = df_final['pmr_um'].apply(lambda x : get_das_tot_distinct(x)))

#le das le plus fréquent

df_final = df_final.assign(das_frequent = df_final['pmr_um'].apply(lambda x : get_das_frequent(x)))

#la frequence du Das frequent

df_final = df_final.assign(frequence_das_freq = df_final['pmr_um'].apply(lambda x : frequence_das_frequent(x) ))

# nombre des RUM avec au moins un acte

df_final = df_final.assign(Rums_avec_actes = df_final['pmr_um'].apply(lambda x : get_rum_with_actes(x) ))

#pourcentage des RUMs qui ont un acte

df_final = df_final.assign(pourcentage_rum_acte = df_final['pmr_um'].apply(lambda x : round(get_rum_with_actes(x) * 100 / get_rum_total(x),1)))
# nombre de actes Distinct par unité médicale
df_final = df_final.assign(actes_total_distinct = df_final['pmr_um'].apply(lambda x : get_act_tot_distinct(x)))

#l'acte le plus fréquent        

df_final = df_final.assign(acte_frequent = df_final['pmr_um'].apply(lambda x : get_acte_frequent(x)))

#la frequence de l'acte frequent

df_final = df_final.assign(frequence_acte_freq = df_final['pmr_um'].apply(lambda x : frequence_act_frequent(x) ))


df_final.columns = ['UM', 'Début', 'Fin', 'Nombre de RUM','Durée Mediane', 'Part Activité', 'Nombre Séances', 'Pourcentage Séances',
  'Nombre DP Différents', 'DP fréquent', 'Fréquence Dp Fréquent', 'Fréquence DP rare',
   'Nombre RUM Avec DR', '% RUM Avec DR','Nombre DR différents', 'DR Fréquent', 'Fréquence DR Fréquent',
  'Nombre RUM Avec DAS', '% RUM Avec DAS', 'Nombre DAS Différents','DAS Fréquent', 'Fréquence DAS Fréquent',
    'Nombre RUM Avec Actes ','% RUM Avec Acte ', 'Nombre ACTE Différents', 'Acte Fréquent','Fréquence Acte Fréquent']


style = """
table {
    border-collapse: collapse;
    box-shadow: 0 5px 50px rgba(0,0,0,0.15);
    cursor: pointer;
    margin: 0px auto;
    border: 2px solid #191970;
}

table th {
  position: sticky;
  top: 0px;
  background-color: #1f263b ;
  color: #fff;
}

thead tr {
    background-color: #1f263b ;
    color: #fff;
    text-align: left;
}

th, td {
    padding: 15px 20px;
    text-align: center;
}

tbody tr, td, th {
    border: 1px solid #ddd;
}

tbody tr:nth-child(even){
    background-color: #f3f3f3;
}

@media screen and (max-width: 550px) {
  body {
    align-items: flex-start;
  }
  table  {
    width: 100%;
    margin: 0px;
    font-size: 10px;
  }
  th, td {
    padding: 10px 7px;
}

}

img{
  display: block;
  margin-left: auto;
  margin-right: auto;
  
}

h1{
  font-weight: bolder ;
  font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  font-size: 250%;
  border: 3px solid black;
  border-radius: 20px;
  width: fit-content;
  padding: 1%;
}

h2, h3, h4{
  font-family:Verdana;
  font-weight: bolder;
}
h2{
  color: #ff4500;
}

h3{
color : #191970;
}


body            
{
    margin:auto;
    padding:10px;
    background-color:#ebebeb;
    font-size:14px;
    font-family:Verdana;
    padding-left: 2%;
    margin-left: 11vh;
}
p{
  font-size: large;
}
.boutton{
  border-radius: 8px;
  color: white;
  background-color: #191970;
}
.boutton:hover{
  cursor: pointer;
  color: white;
  background-color: #ff4500;
}

/************************/

.outer-wrapper {
  max-width: fit-content;
  max-height: fit-content;
  margin: auto;
}
.table-wrapper {

  overflow-y: scroll;
  overflow-x: scroll;
  height: fit-content;
  max-height: 66.4vh;
  max-width: 130vh;
  margin-top: 22px;
  
  margin: 15px;
  padding-bottom: 20px;

}

/***********************************************************/
.main-menu{
  background-color: #fff;
  border-top-right-radius: 10px;
  border-bottom-right-radius: 10px;
  box-shadow: 0px 0px 20px #d4d4d4;

  width: 70px;
  height: 100vh;

  position: fixed;
  top:0;
  left:0;
  
  overflow-x: hidden;
  overflow-y: hidden;
  white-space: nowrap;
  
  transition: .3s ease;
  font-family: 'Fira Sans', sans-serif;
}

.main-menu:hover{
  width : 140px;
  overflow-y: scroll;
}

.main-menu .menu-item{
  padding: 10px;
  margin-left: 20px;
}
.main-menu .menu-item:hover{
  background-color: #d6d6d6;
  cursor: pointer;
}

.main-menu .menu-item .fa{
  color: gray;
  width:40px;
  font-size: 25px;
  padding: 10px;
}

.main-menu .menu-item:hover > .fa{
  color: #1d0daa;
}

::-webkit-scrollbar {
  width: 5px;
}

::-webkit-scrollbar-track {
  background: transparent !important;
}

::-webkit-scrollbar-thumb {
  background: #bbbbbb;
  border-radius: 10px;
}

a{
  font-family: 'Trebuchet MS', sans-serif;
  color: black;
  font-size:medium;
  font-weight: bolder;
}

/***********************************************************/

.table-sortable th {
  cursor: pointer;
}

.table-sortable .th-sort-asc::after {
  content: "\\25b4";
}

.table-sortable .th-sort-desc::after {
  content: "\\25be";
}

.table-sortable .th-sort-asc::after,
.table-sortable .th-sort-desc::after {
  margin-left: 5px;
}

.table-sortable .th-sort-asc,
.table-sortable .th-sort-desc {
  background:  #ff4500 ;
}

.menuu {
  overflow:auto;
  text-align: center;
}

.menuu li {
  padding: 8px;
  width: 10%; /* Four links of equal widths */
  text-align: center;
  display: inline-block;
  border: 1px solid black;
  border-radius : 20px;
  margin: 8px;
  transition: all .5s ease;
  font-weight: bold;
}

.menuu li:hover {
  background-color: #ff4500;
  cursor: pointer;
  transition: all .5s ease;
}


@media screen and (max-width: 500px) {
  .menuu li {
    float: none;
    display: block;
    width: 100%;
    text-align: left;
  }
}

#tableau1{
  visibility:visible;
  position: absolute;
  margin-left: 30vh;
  margin-right: 30vh;
}

#tableau2{
  visibility: hidden;
  position: absolute;
  margin-left: 30vh;
  margin-right: 30vh;
  text-align: center;
}

#tableau3{
  visibility: hidden;
  position: absolute;
  margin-left: 30vh;
  margin-right: 30vh;
}

#tableau4{
  visibility: hidden;
  position: absolute;
  margin-left: 30vh;
  margin-right: 30vh;
}

#tableau5{
  visibility: hidden;
  position: absolute;
  margin-left: 30vh;
  margin-right: 25vh;
}
/**********************************************/

        """
css = open("rapports/style.css", 'w')
css.writelines(style)
css.close()    

js= """
 
    /**
 * Sorts a HTML table.
 * 
 * @param {HTMLTableElement} table The table to sort
 * @param {number} column The index of the column to sort
 * @param {boolean} asc Determines if the sorting will be in ascending
 */
function sortTableByColumn(table, column, asc = true) {
    const dirModifier = asc ? 1 : -1;
    const tBody = table.tBodies[0];
    const rows = Array.from(tBody.querySelectorAll("tr"));

    // Sort each row
    const sortedRows = rows.sort((a, b) => {
        const aColText = a.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim();
        const bColText = b.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim();
        
        
    if (isNaN(String(aColText)) && isNaN(String(bColText))) {
      return aColText > bColText ? (1 * dirModifier) : (-1 * dirModifier);
    }
    return parseFloat(aColText) > parseFloat(bColText) ? (1 * dirModifier) : (-1 * dirModifier);

  });
 

    // Remove all existing TRs from the table
    while (tBody.firstChild) {
        tBody.removeChild(tBody.firstChild);
    }

    // Re-add the newly sorted rows
    tBody.append(...sortedRows);

    // Remember how the column is currently sorted
    table.querySelectorAll("th").forEach(th => th.classList.remove("th-sort-asc", "th-sort-desc"));
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-asc", asc);
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-desc", !asc);
}

document.querySelectorAll(".table-sortable th").forEach(headerCell => {
    headerCell.addEventListener("click", () => {
      
        const tableElement = headerCell.closest('.table-sortable'); 
        const headerIndex = Array.prototype.indexOf.call(headerCell.parentElement.children, headerCell);
        const currentIsAscending = headerCell.classList.contains("th-sort-asc");

        sortTableByColumn(tableElement, headerIndex, !currentIsAscending);
    });
});

  function openLink() {
    
    var input = document.getElementById("in").value;
    
    if (input.length == 0)
      { 
         alert("Veuillez renseigner une UM valide");  	
         return false;
      }  	
    window.open("u_medicales/"+input+"/"+input+".html");   
      }

function change(contexte){
  var tableau1 = document.getElementById("tableau1");
  var tableau2 = document.getElementById("tableau2");
  var tableau3 = document.getElementById("tableau3");
  var tableau4 = document.getElementById("tableau4");
  var tableau5 = document.getElementById("tableau5");

  if (contexte !== true){
    if(contexte.target.value == 1){
      tableau1.style.visibility = "visible";
      tableau2.style.visibility = "hidden";
      tableau3.style.visibility = "hidden";
      tableau4.style.visibility = "hidden";
      tableau5.style.visibility = "hidden";

    }
    if(contexte.target.value == 2){
      tableau1.style.visibility = "hidden";
      tableau2.style.visibility = "visible";
      tableau3.style.visibility = "hidden";
      tableau4.style.visibility = "hidden";
      tableau5.style.visibility = "hidden";

    }
    if(contexte.target.value == 3){
      tableau1.style.visibility = "hidden";
      tableau2.style.visibility = "hidden";
      tableau3.style.visibility = "visible";
      tableau4.style.visibility = "hidden";
      tableau5.style.visibility = "hidden";

    }
    if(contexte.target.value == 4){
      tableau1.style.visibility = "hidden";
      tableau2.style.visibility = "hidden";
      tableau3.style.visibility = "hidden";
      tableau4.style.visibility = "visible";
      tableau5.style.visibility = "hidden";

    }
    if(contexte.target.value == 5){
      tableau1.style.visibility = "hidden";
      tableau2.style.visibility = "hidden";
      tableau3.style.visibility = "hidden";
      tableau4.style.visibility = "hidden";
      tableau5.style.visibility = "visible";

    }
  }
}
"""
js_f = open("rapports/script.js", 'w')
js_f.writelines(js)
js_f.close() 
#le fichier html index.html _ début d'écriture  
du_au = "du "+to_french_date(pmsi_rum["pmr_startdate"].min()) +' au '+ to_french_date(pmsi_rum["pmr_enddate"].max())
total_ums = len(pmsi_rum.groupby('pmr_um')) 
total_rums_k = round(pmsi_rum.shape[0]  /1000 , 1)
seances_perc = round(pmsi_rum[pmsi_rum["ghm"].str.match('^28.*')== True].shape[0] * 100/ pmsi_rum.shape[0] , 1)
seances_rums_k = round(pmsi_rum[pmsi_rum["ghm"].str.match('^28.*')== True].shape[0] /1000 , 1)
act_negli2 = len(df_final[df_final['Nombre de RUM'] <= negl_nbr_rum])

index = """

<HTML>
<HEAD><TITLE>Analyse Statistiques Descriptives</TITLE>           
<link rel="stylesheet" href = "style.css">

</HEAD>
<body >

<DIV ALIGN="center">
<IMG SRC="https://www.alicante.healthcare/wp-content/uploads/2021/10/logo-alicante-orange-degrade.png" width = "500" height = "151">

</DIV>
<br>                                               

<DIV ALIGN="center">
<h1><B><U>Analyse Statistiques Descriptives</B></U></h1>
</DIV>     
<h2><B><U>Données</B></U></h2>
        <ul>
            <li>Client : {client}</li>
            <li>Données : PMSI  </li>
            <li>Période : {du_au}</li>
            <li>Nombre d’UMs : {total_ums} </li>    
            <li>Nombre de RUM : ~ {total_rums_k}K </li>
            <li>Pourcentage de séances médicales : {seances_perc}% (~{seances_rums_k}K)</li>
        </ul>
<div>
    <input type="text" placeholder="Chercher Par UM" id="in">
    <button type="button" onclick="openLink();">Afficher les Statistiques</button>    

</div>
<div class="menuu">
  <ul>
    <li onclick="change(event);" value="1">Global</li>
    <li onclick="change(event);" value="2">DP</li>
    <li onclick="change(event);" value="3">DR</li>
    <li onclick="change(event);" value="4">DAS</li>
    <li onclick="change(event);" value="5">CCAM</li>

  </ul>
</div>
<div class="main-menu"><ul>
 """.format(**locals())
ecriture = open("rapports/index.html", 'w')
ecriture.writelines(index)


def generer(um_i):
        
    #création de dossier de chaque unité médicale avec un dossier de pgraphiques
    if not os.path.exists('rapports/u_medicales/'+str(um_i)+'/plots'):
        os.makedirs('rapports/u_medicales/'+str(um_i)+'/plots')
  
    #global df_final_2
    # les variables
    lien = "u_medicales/"+str(um_i)+"/"+str(um_i)+".html"
    #les codes dont apparaition est moins de n fois
    dp_max =  int(variables['dp_max'])
    
    
    unite = "UM "+str(um_i)
    
    min_dp_apparition = get_dp_rare_count(um_i)
    
    get_rum_total_var = get_rum_total(um_i)
    get_median_duree_var = get_median_duree(um_i)
    duree_rums = duration(um_i).to_html()
    get_dp_tot_distinct_var = get_dp_tot_distinct(um_i)
    get_dp_frequent_var = get_dp_frequent(um_i)
    get_dp_frequent_count_var = get_dp_frequent_count(um_i)
    frequence_dp_freq_var = frequence_dp_freq(um_i)
    frequence_dp_rare_dyn_var = frequence_dp_rare(um_i)
    
    get_dr_total_var = get_dr_total(um_i)
                     
    if not get_dr_total_var :
        dp_dr_var = "Aucun DR dans cette UM"
        infos_dr = ""

    else:
        get_dr_tot_distinct_var = get_dr_tot_distinct(um_i)
        get_dr_frequent_var = get_dr_frequent(um_i)
        get_dr_frequent_count_var = get_dr_frequent_count(um_i)
        frequence_dr_frequent_var = frequence_dr_frequent(um_i)
        dp_dr_var = pd.DataFrame(dp_dr(um_i)).to_html(index=False)
        infos_dr = """<p>Le nombre des différents DR est:  <b>{get_dr_tot_distinct_var} </b></p>     

        <p>Le DR le plus fréquent de cette UM est : <b>{get_dr_frequent_var}</b> avec <b>{get_dr_frequent_count_var} </b> fois soit une fréquence de :
        <b> {frequence_dr_frequent_var}%</b></p>   
        <img src ="plots/most_dR_frequents.png"> """.format(**locals())

  
    
    get_das_total_var = get_das_total(um_i)
    if not get_das_total_var :
        infos_das = ""
    else:
        get_das_tot_distinct_var = get_das_tot_distinct(um_i)
        get_das_frequent_var = get_das_frequent(um_i)        
        get_das_frequent_count_var = get_das_frequent_count(um_i)
        get_das_frequent_freq = frequence_das_frequent(um_i)
        infos_das ="""
        <p>Le nombre des différents DAS est: <b> {get_das_tot_distinct_var}</b> </p>     
    
        <p>Le DAS le plus fréquent de cette unité médicale est : <b> {get_das_frequent_var}</b> avec <b>{get_das_frequent_count_var}</b> fois soit une fréquence de :<b> {get_das_frequent_freq}%</b></p>   
         <img src ="plots/most_das_frequents.png">            
        
        """.format(**locals())
        
    
    get_rums_act = get_rum_with_actes(um_i)
    if not get_rums_act :
        infos_act = ""
    else:
        get_act_tot_distinct_var = get_act_tot_distinct(um_i)
        get_acte_frequent_var = get_acte_frequent(um_i)      
        count_act_freq = get_act_frequent_count(um_i)
        frequence_act_frequent_var = frequence_act_frequent(um_i)
        infos_act = """<p>Le nombre des différents actes dans UM est :  <b> {get_act_tot_distinct_var} </b> </p>   
            <p>L'acte le plus fréquent de cette unité médicale est : <b> {get_acte_frequent_var} </b> avec <b>{count_act_freq} </b>fois, soit une fréquence de :
             <b> {frequence_act_frequent_var} </b> </p>     
             <img src ="plots/most_actes_frequents.png"> 
            """.format(**locals())
    
    
    #les plots pour sauvegarder les figures 
           
    plot_most_dp_frequents(um_i )         
    plot_most_dr_frequents(um_i)   
    plot_most_das_frequents(um_i)       
    plot_most_act_frequents(um_i ) 


# diviser les stats en tableaux selon infos global, partie DP, partie DR, Partie DAS, Partie CCAM  
    um_glob = df_final[df_final['UM'] == um_i]
    um_glob = um_glob[['UM', 'Début', 'Fin', 'Nombre de RUM','Durée Mediane', 'Part Activité', 'Nombre Séances', 'Pourcentage Séances']]
    um_glob.set_index("UM", inplace = False) 
    um_glob =  um_glob.to_html(index=False)
    
                    
    um_dp = df_final[df_final['UM'] == um_i]
    um_dp = um_dp[['UM','Part Activité','Nombre Séances','Nombre DP Différents', 'DP fréquent', 'Fréquence Dp Fréquent', 'Fréquence DP rare']]
    um_dp.set_index("UM", inplace = False) 
    um_dp = um_dp.to_html(index=False)
                    
    um_dr = df_final[df_final['UM'] == um_i]    
    um_dr = um_dr[['UM','Part Activité','Nombre Séances','Nombre RUM Avec DR', '% RUM Avec DR','Nombre DR différents', 'DR Fréquent', 'Fréquence DR Fréquent']]
    um_dr.set_index("UM", inplace = False) 
    um_dr = um_dr.to_html(index=False)
                    
    um_das = df_final[df_final['UM'] == um_i]
    um_das = um_das[['UM','Part Activité','Nombre Séances','Nombre RUM Avec DAS', '% RUM Avec DAS', 'Nombre DAS Différents','DAS Fréquent', 'Fréquence DAS Fréquent']]
    um_das.set_index("UM", inplace = False) 
    um_das = um_das.to_html(index=False)

    um_acte = df_final[df_final['UM'] == um_i]
    um_acte = um_acte [['UM','Part Activité','Nombre Séances', 'Nombre RUM Avec Actes ','% RUM Avec Acte ', 'Nombre ACTE Différents', 'Acte Fréquent','Fréquence Acte Fréquent']]
    um_acte.set_index("UM", inplace = False) 
    um_acte = um_acte.to_html(index=False)    
    
        
    # fichier html de chaque unité médicale:   
    template_um = """
  

           
   <HTML>
    <HEAD><TITLE>Analyse Statistique des unités médicales</TITLE>    
    <link rel="stylesheet" href= "../../style.css">          

    </HEAD>
    <BODY>

    <DIV ALIGN="center">
    <IMG SRC="https://www.alicante.healthcare/wp-content/uploads/2021/10/logo-alicante-orange-degrade.png" width = "500" height = "151">
    <h1>Analyse Statistique des unités médicales</h1>
    </DIV>
    
    <a href = "../../index.html" ><input type="button" value="Retour" class="boutton"></input></a>
    <h2>L'unité médicale {um_i} </h2></p>            

    <p>Le nombre total de RUM dans cette UM est: <b> {get_rum_total_var}. </b> </p>
    <p>La durée mediane de séjour est de : <b> {get_median_duree_var} </b></p>     
    {duree_rums}               
    <h3>Le Diagnostic principal : </h3> 
    <p>Le nombre des différents DP est: <b> {get_dp_tot_distinct_var} </b> </p>  
    <p>Le DP le plus fréquent de cette UM est : <b> {get_dp_frequent_var}</b> avec <b>{get_dp_frequent_count_var}</b> fois, soit une fréquence de : <b>{frequence_dp_freq_var}% </b></p>    

    <p> Le nombre minimum d'apparation des DP est : <b>{min_dp_apparition}</b></p>
    <p>La fréquence de DP rares qui apparaissent <b>{dp_max} </b> fois est : <b>{frequence_dp_rare_dyn_var} </b></p>  
    
    <img src ="plots/most_dp_frequents.png">                
     <h3>Le Diagnostic Relié : </h3>  
    <p>Le nombre de RUM avec un DR est : <b>{get_dr_total_var}</b> </p>    
    {infos_dr} 
        
   <h3>Le Diagnostic Asocié : </h3> 
    <p>Le nombre de RUM avec au moins un DAS : <b> {get_das_total_var}</b> </p>         

    {infos_das}
   
    <h3>Les actes : </h3>    
    <p>Le nombre total de Résumés médicales avec au moins un acte est: <b>  {get_rums_act} </b> </p>  
    {infos_act}  
    
    <h3>Les couples dp et dr qui viennent ensemble dans cette unité :</h3>  

    {dp_dr_var}    

    <h3> Résumé de l'activité globale</h3>
    <div class='outer-wrapper'>
    <div class='table-wrapper'>
    {um_glob}
    </div></div>
        
    <h3> Résumé de l'activité globale en termes de diagnostic principal </h3>
    <div class='outer-wrapper'>
    <div class='table-wrapper'>
    {um_dp}
    </div></div>
    
    <h3> Résumé de l'activité globale en termes de diagnostic relié</h3>
    <div class='outer-wrapper'>
    <div class='table-wrapper'>
    {um_dr}
    </div></div>
    
    <p><h3>  Résumé de l'activité globale en termes de diagnostics associés </p></h3>
    <div class='outer-wrapper'>
    <div class='table-wrapper'>
    {um_das}
    </div></div>
    
    <p><h3> Résumé de l'activité globale en termes des actes CCAM</p></h3>
    <div class='outer-wrapper'>
    <div class='table-wrapper'>
    {um_acte}  
    </div></div>
    
    <a href = "../../index.html" ><input type="button" value="Retour" class="boutton"></input></a>             

    </BODY></HTML> """.format(**locals())         

          
    # érciture de lien de chaque unité médicale
    ecriture.writelines("<li class='menu-item'><a href = "+lien+" >"+unite+"</a></li>")     

        
    with open('rapports/u_medicales/'+str(um_i)+'/'+str(um_i)+".html", 'w') as f :
        f.write(template_um)
        f.close()
          


#appliquer le code generer pour chaque unité choisie sinon toutes
UM['pmr_um'].apply(lambda x : generer(x))

df_final['UM'] = df_final['UM'].apply(lambda x : activer_lien(x))

res_glob = df_final[['UM', 'Début', 'Fin', 'Nombre de RUM','Durée Mediane', 'Part Activité', 'Nombre Séances', 'Pourcentage Séances']]

res_glob = res_glob.to_html(index=False,  classes = "table-sortable", escape=False)


res_dp = df_final[['UM','Part Activité','Nombre Séances','Nombre DP Différents', 'DP fréquent', 'Fréquence Dp Fréquent', 'Fréquence DP rare']]

res_dp = res_dp.to_html(index=False , classes = "table-sortable", escape=False)

res_dr = df_final[['UM','Part Activité','Nombre Séances','Nombre RUM Avec DR', '% RUM Avec DR','Nombre DR différents', 'DR Fréquent', 'Fréquence DR Fréquent']]

res_dr = res_dr.to_html(index=False , classes = "table-sortable", escape=False)

res_das = df_final[['UM','Part Activité','Nombre Séances','Nombre RUM Avec DAS', '% RUM Avec DAS', 'Nombre DAS Différents','DAS Fréquent', 'Fréquence DAS Fréquent']]

res_das = res_das.to_html(index=False , classes = "table-sortable", escape=False)

res_acte = df_final[['UM','Part Activité','Nombre Séances', 'Nombre RUM Avec Actes ','% RUM Avec Acte ', 'Nombre ACTE Différents', 'Acte Fréquent','Fréquence Acte Fréquent']]

res_acte = res_acte.to_html(index=False , classes = "table-sortable", escape=False)

ecriture.writelines("</ul></div>")       

ecriture.writelines("<div id='tableau1'>")
ecriture.writelines("<p><h2>  Résumé de l'activité globale </p></h2>")
ecriture.writelines("<div class='outer-wrapper'>")
ecriture.writelines("<div class='table-wrapper'>")
ecriture.writelines(res_glob)
ecriture.writelines("</div></div>")
ecriture.writelines("</div><div id='tableau2'>")

ecriture.writelines("<p><h2> Résumé de l'activité globale en termes de diagnostics principaux </p></h2>")
ecriture.writelines("<div class='outer-wrapper'>")
ecriture.writelines("<div class='table-wrapper'>")
ecriture.writelines(res_dp)
ecriture.writelines("</div></div>")
ecriture.writelines("</div><div id='tableau3'>")

ecriture.writelines("<p><h2> Résumé de l'activité globale en termes de diagnostics reliés</p></h2>")
ecriture.writelines("<div class='outer-wrapper'>")
ecriture.writelines("<div class='table-wrapper'>")
ecriture.writelines(res_dr)
ecriture.writelines("</div></div>")
ecriture.writelines("</div><div id='tableau4'>")

ecriture.writelines("<p><h2>  Résumé de l'activité globale en termes de diagnostics associés </p></h2>")
ecriture.writelines("<div class='outer-wrapper'>")
ecriture.writelines("<div class='table-wrapper'>")
ecriture.writelines(res_das)
ecriture.writelines("</div></div>")
ecriture.writelines("</div><div id='tableau5'>")

ecriture.writelines("<p><h2> Résumé de l'activité globale en termes des actes CCAM</p></h2>")
ecriture.writelines("<div class='outer-wrapper'>")
ecriture.writelines("<div class='table-wrapper'>")
ecriture.writelines(res_acte) 
ecriture.writelines("</div></div></div>")


ecriture.writelines("<script src= 'script.js'></script>")  
ecriture.writelines("</BODY></HTML>")
ecriture.close()





