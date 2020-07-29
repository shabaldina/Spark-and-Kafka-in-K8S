import requests
import json
import pywebhdfs.webhdfs
import time
import quandl
import pandas
import datetime
import os
import socket
from kafka import KafkaProducer
import threading


# This class starts a deamonized thread to do handle all import tasks
# All import logic is part of this class
class HDFSImportJob:
    def __init__(self, hdfsconn, quandl_companies_list, quandl_hdfs_export_path, quandl_start_date, infections_import_url, infection_hdfs_export_path, kafka_producer):
        self.hdfsconnection = hdfsconn
        self.quandlcompanieslist = quandl_companies_list
        self.quandlhdfsexportpath = quandl_hdfs_export_path
        self.quandlstartdate = quandl_start_date
        self.infectionimporturl = infections_import_url
        self.infectionhdfsexportpath = infection_hdfs_export_path
        self.kafkaproducer = kafka_producer
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    # Main Method. Is executed in a deaomized thread
    def run(self):
        while True:
            print("Executing Import Job")
            # Always reset the current Date
            self.quandlenddate = datetime.date.today().strftime("%Y-%m-%d")
            # Import Frankfurt Stock Exchange
            self.import_fse()
            print("Finished stock import. Starting infections import")
            # Import COVID-19 Infection-Data
            self.import_infections()
            print("Import Job Finished, sending Message to Kafka")
            # Send Message to Spark Framework via Kafka if producer is defined
            if self.kafkaproducer == None:
                try:
                    self.kafkaproducer = KafkaProducer(bootstrap_servers='my-cluster-kafka-bootstrap:9092')
                except:
                    print("Unable to connect to Kafka-Service")
                    self.kafkaproducer = None

            if producer != None:
                try:
                    self.kafkaproducer.send("spark_notification", "new_data_available".encode()).get(timeout=5)
                except:
                    print("Unable to send notification to Spark through Kafka-Service")
            
            print("Sleeping 1h")
            time.sleep(3600)

    # This Method imports the newest values from the Frankfurt Stock Exchange
    # The Data is provided by Quandl
    def import_fse(self):
        # Define hdfs path of result file
        hdfs_export_filename = self.quandlhdfsexportpath + "/quandl_fse.csv"
        resultdf = pandas.DataFrame()

        # Quandl API requires seperate requests for each Stock Exchange Company
        for company in self.quandlcompanieslist:
            # Fetch Data from Quandl Website
            data = quandl.get("FSE/" + company, start_date = self.quandlstartdate, end_date = self.quandlenddate)
            data['Share'] = company
            # Append fetched data to main DataFrame
            resultdf = pandas.concat([resultdf, data])
            print("Added company: " + company)
        
        # Reorder Columns for better readability
        resultdf = resultdf[['Share','Open','High','Low', 'Close', 'Change', 'Traded Volume', 'Turnover', 'Last Price of the Day', 'Daily Traded Units','Daily Turnover']]
        
        # Group by Company name
        resultdf.groupby('Share')
        
        # Check if there is already a file existing and delete it
        if self.hdfsconnection.exists_file_dir(hdfs_export_filename):
            self.hdfsconnection.delete_file_dir(hdfs_export_filename)

        # Create new File with current output
        self.hdfsconnection.create_file(hdfs_export_filename, resultdf.to_csv(sep=";",index=True, line_terminator='\n'), permission=777)

    # Import newest infection statistics for COVID-19 from ECDC via Web API
    def import_infections(self):
        # Define hdfs path of result file
        hdfs_export_filename = self.infectionhdfsexportpath + "/infections.csv"
        
        # Fetch JSON with HTTP-Request
        response = requests.get(self.infectionimporturl)
        # We need to access text property of response object to make the new encoding take affect
        response.encoding = 'utf-8'
        responsetext = response.text
        jsonresponse = json.loads(responsetext, encoding='utf-8')
                
        # Transform JSON to Dataframe
        responsedf = pandas.json_normalize(jsonresponse["records"])
        # Remove this countries because its causing encoding issues with pywebhdfs
        responsedf = responsedf[responsedf.countriesAndTerritories != 'Curaçao']

        # Check if there is already a file existing and delete it
        if self.hdfsconnection.exists_file_dir(hdfs_export_filename):
            self.hdfsconnection.delete_file_dir(hdfs_export_filename)
        
        # Create new File with current output
        self.hdfsconnection.create_file(hdfs_export_filename, responsedf.to_csv(sep=";",index=False, line_terminator='\n'), permission=777)


# Get IP of Knox Service by DNSLookup
hdfsweburl = "http://" + str(socket.gethostbyname("knox-apache-knox-helm-svc")) + ":8080"
# Connect to Kafka as a Producer
try:
    producer = KafkaProducer(bootstrap_servers='my-cluster-kafka-bootstrap:9092')
except:
    print("Unable to connect to Kafka-Service")
    producer = None


# We only want Data beginning from 2020
startdate = "2020-01-01"
# Hard-Coded list of all Frankfurt Stock Exchange companies supplied by Quandl
quandl_companies = json.loads("{\"1COV_X\": [\"1COV_X\", \"Covestro AG (1COV_X)\"], \"2HR_X\": [\"2HR_X\", \"H&R (2HR_X)\"], \"AAD_X\": [\"AAD_X\", \"Amadeus FiRe AG (AAD_X)\"], \"AB1_X\": [\"AB1_X\", \"Air Berlin Plc (AB1_X)\"], \"ADS_X\": [\"ADS_X\", \"Adidas (ADS_X)\"], \"ADV_X\": [\"ADV_X\", \"Adva Se (ADV_X)\"], \"AFX_X\": [\"AFX_X\", \"Carl Zeiss Meditec (AFX_X)\"], \"AIR_X\": [\"AIR_X\", \"Airbus Group (eads N.v.) (AIR_X)\"], \"AIXA_X\": [\"AIXA_X\", \"Aixtron Se (AIXA_X)\"], \"ALV_X\": [\"ALV_X\", \"Allianz Se (ALV_X)\"], \"ANN_X\": [\"ANN_X\", \"Deutsche Annington Immobilien Se (ANN_X)\"], \"AOX_X\": [\"AOX_X\", \"alstria office REIT-AG (AOX_X)\"], \"ARL_X\": [\"ARL_X\", \"Aareal Bank (ARL_X)\"], \"B5A_X\": [\"B5A_X\", \"Bauer Aktiengesellschaft (B5A_X)\"], \"BAF_X\": [\"BAF_X\", \"Balda (BAF_X)\"], \"BAS_X\": [\"BAS_X\", \"Basf Se (BAS_X)\"], \"BAYN_X\": [\"BAYN_X\", \"Bayer (BAYN_X)\"], \"BBZA_X\": [\"BBZA_X\", \"Bb Biotech (BBZA_X)\"], \"BC8_X\": [\"BC8_X\", \"Bechtle (BC8_X)\"], \"BDT_X\": [\"BDT_X\", \"Bertrandt (BDT_X)\"], \"BEI_X\": [\"BEI_X\", \"Beiersdorf Aktiengesellschaft (BEI_X)\"], \"BIO3_X\": [\"BIO3_X\", \"Biotest  Vz (BIO3_X)\"], \"BMW_X\": [\"BMW_X\", \"Bmw St (BMW_X)\"], \"BNR_X\": [\"BNR_X\", \"Brenntag (BNR_X)\"], \"BOSS_X\": [\"BOSS_X\", \"Hugo Boss (BOSS_X)\"], \"BYW6_X\": [\"BYW6_X\", \"Baywa Vna (BYW6_X)\"], \"CBK_X\": [\"CBK_X\", \"Commerzbank (CBK_X)\"], \"CEV_X\": [\"CEV_X\", \"Centrotec Sustainable (CEV_X)\"], \"CLS1_X\": [\"CLS1_X\", \"Celesio (CLS1_X)\"], \"COK_X\": [\"COK_X\", \"Cancom Se (COK_X)\"], \"COM_X\": [\"COM_X\", \"comdirect bank AG (COM_X)\"], \"CON_X\": [\"CON_X\", \"Continental (CON_X)\"], \"COP_X\": [\"COP_X\", \"Compugroup Medical (COP_X)\"], \"CWC_X\": [\"CWC_X\", \"Cewe Stiftung & Co. Kgaa (CWC_X)\"], \"DAI_X\": [\"DAI_X\", \"Daimler (DAI_X)\"], \"DB1_X\": [\"DB1_X\", \"Deutsche B\u00c3\u00b6rse (DB1_X)\"], \"DBAN_X\": [\"DBAN_X\", \"Deutsche Beteiligungs AG (DBAN_X)\"], \"DBK_X\": [\"DBK_X\", \"Deutsche Bank (DBK_X)\"], \"DEQ_X\": [\"DEQ_X\", \"Deutsche Euroshop (DEQ_X)\"], \"DEX_X\": [\"DEX_X\", \"Delticom (DEX_X)\"], \"DEZ_X\": [\"DEZ_X\", \"Deutz (DEZ_X)\"], \"DIC_X\": [\"DIC_X\", \"DIC Asset AG (DIC_X)\"], \"DLG_X\": [\"DLG_X\", \"Dialog Semiconductor Plc (DLG_X)\"], \"DPW_X\": [\"DPW_X\", \"Deutsche Post (DPW_X)\"], \"DRI_X\": [\"DRI_X\", \"Drillisch (DRI_X)\"], \"DRW3_X\": [\"DRW3_X\", \"Dr\u00c3\u00a4gerwerk  & Co. Kgaa Vz (DRW3_X)\"], \"DTE_X\": [\"DTE_X\", \"Deutsche Telekom (DTE_X)\"], \"DUE_X\": [\"DUE_X\", \"D\u00c3\u00bcrr (DUE_X)\"], \"DWNI_X\": [\"DWNI_X\", \"Deutsche Wohnen (DWNI_X)\"], \"EOAN_X\": [\"EOAN_X\", \"E.on Se (EOAN_X)\"], \"EON_X\": [\"EON_X\", \"E.on Se (EON_X)\"], \"EVD_X\": [\"EVD_X\", \"CTS Eventim (EVD_X)\"], \"EVK_X\": [\"EVK_X\", \"Evonik Industries (EVK_X)\"], \"EVT_X\": [\"EVT_X\", \"Evotec (EVT_X)\"], \"FIE_X\": [\"FIE_X\", \"Fielmann (FIE_X)\"], \"FME_X\": [\"FME_X\", \"Fresenius Medical Care  & Co. Kgaa St (FME_X)\"], \"FNTN_X\": [\"FNTN_X\", \"Freenet (FNTN_X)\"], \"FPE3_X\": [\"FPE3_X\", \"Fuchs Petrolub  Vz (FPE3_X)\"], \"FRA_X\": [\"FRA_X\", \"Fraport (FRA_X)\"], \"FRE_X\": [\"FRE_X\", \"Fresenius Se & Co. Kgaa (FRE_X)\"], \"G1A_X\": [\"G1A_X\", \"GEA AG (G1A_X)\"], \"GBF_X\": [\"GBF_X\", \"Bilfinger Se (GBF_X)\"], \"GFJ_X\": [\"GFJ_X\", \"Gagfah S.a. (GFJ_X)\"], \"GFK_X\": [\"GFK_X\", \"Gfk Se (GFK_X)\"], \"GIL_X\": [\"GIL_X\", \"Dmg Mori Seiki (GIL_X)\"], \"GLJ_X\": [\"GLJ_X\", \"Grenkeleasing (GLJ_X)\"], \"GMM_X\": [\"GMM_X\", \"Grammer (GMM_X)\"], \"GSC1_X\": [\"GSC1_X\", \"Gesco (GSC1_X)\"], \"GWI1_X\": [\"GWI1_X\", \"Gerry Weber International (GWI1_X)\"], \"GXI_X\": [\"GXI_X\", \"Gerresheimer (GXI_X)\"], \"HAB_X\": [\"HAB_X\", \"Hamborner Reit AG (HAB_X)\"], \"HAW_X\": [\"HAW_X\", \"Hawesko Holding (HAW_X)\"], \"HBH3_X\": [\"HBH3_X\", \"Hornbach Holding Ag (HBH3_X)\"], \"HDD_X\": [\"HDD_X\", \"Heidelberger Druckmaschinen (HDD_X)\"], \"HEI_X\": [\"HEI_X\", \"Heidelbergcement (HEI_X)\"], \"HEN3_X\": [\"HEN3_X\", \"Henkel  & Co. Kgaa Vz (HEN3_X)\"], \"HHFA_X\": [\"HHFA_X\", \"HHLA AG (Hamburger Hafen und Logistik) (HHFA_X)\"], \"HNR1_X\": [\"HNR1_X\", \"Hannover R\u00c3\u00bcck Se (HNR1_X)\"], \"HOT_X\": [\"HOT_X\", \"Hochtief (HOT_X)\"], \"IFX_X\": [\"IFX_X\", \"Infineon Technologies (IFX_X)\"], \"INH_X\": [\"INH_X\", \"Indus Holding (INH_X)\"], \"JEN_X\": [\"JEN_X\", \"Jenoptik (JEN_X)\"], \"JUN3_X\": [\"JUN3_X\", \"Jungheinrich (JUN3_X)\"], \"KBC_X\": [\"KBC_X\", \"Kontron (KBC_X)\"], \"KCO_X\": [\"KCO_X\", \"Kl\u00c3\u00b6ckner & Co (Kl\u00c3\u00b6Co) (KCO_X)\"], \"KD8_X\": [\"KD8_X\", \"Kabel Deutschland Holding (KD8_X)\"], \"KGX_X\": [\"KGX_X\", \"Kion Group (KGX_X)\"], \"KRN_X\": [\"KRN_X\", \"Krones (KRN_X)\"], \"KU2_X\": [\"KU2_X\", \"Kuka Aktiengesellschaft (KU2_X)\"], \"KWS_X\": [\"KWS_X\", \"Kws Saat (KWS_X)\"], \"LEG_X\": [\"LEG_X\", \"Leg Immobilien (LEG_X)\"], \"LEO_X\": [\"LEO_X\", \"Leoni (LEO_X)\"], \"LHA_X\": [\"LHA_X\", \"Deutsche Lufthansa (LHA_X)\"], \"LIN_X\": [\"LIN_X\", \"Linde (LIN_X)\"], \"LPK_X\": [\"LPK_X\", \"LPKF Laser & Electronics AG (LPK_X)\"], \"LXS_X\": [\"LXS_X\", \"Lanxess (LXS_X)\"], \"MAN_X\": [\"MAN_X\", \"Man Se St (MAN_X)\"], \"MEO_X\": [\"MEO_X\", \"Metro  St (MEO_X)\"], \"MLP_X\": [\"MLP_X\", \"Mlp (MLP_X)\"], \"MOR_X\": [\"MOR_X\", \"Morphosys (MOR_X)\"], \"MRK_X\": [\"MRK_X\", \"Merck Kgaa (MRK_X)\"], \"MTX_X\": [\"MTX_X\", \"MTU Aero Engines AG (MTX_X)\"], \"MUV2_X\": [\"MUV2_X\", \"M\u00c3\u00bcnchener R\u00c3\u00bcckversicherungs-Gesellschaft AG (Munich Re) (MUV2_X)\"], \"NDA_X\": [\"NDA_X\", \"Aurubis (NDA_X)\"], \"NDX1_X\": [\"NDX1_X\", \"Nordex Se (NDX1_X)\"], \"NEM_X\": [\"NEM_X\", \"Nemetschek (NEM_X)\"], \"NOEJ_X\": [\"NOEJ_X\", \"NORMA Group SE (NOEJ_X)\"], \"O1BC_X\": [\"O1BC_X\", \"Xing (O1BC_X)\"], \"O2C_X\": [\"O2C_X\", \"Petro Welt AG (ex cat oil) (O2C_X)\"], \"O2D_X\": [\"O2D_X\", \"Telefonica Deutschland AG (O2) (O2D_X)\"], \"OSR_X\": [\"OSR_X\", \"Osram Licht (OSR_X)\"], \"P1Z_X\": [\"P1Z_X\", \"Patrizia Immobilien (P1Z_X)\"], \"PFV_X\": [\"PFV_X\", \"Pfeiffer Vacuum Technology (PFV_X)\"], \"PMOX_X\": [\"PMOX_X\", \"Prime Office (PMOX_X)\"], \"PSAN_X\": [\"PSAN_X\", \"PSI Software AG (PSAN_X)\"], \"PSM_X\": [\"PSM_X\", \"Prosiebensat.1 Media (PSM_X)\"], \"PUM_X\": [\"PUM_X\", \"Puma Se (PUM_X)\"], \"QIA_X\": [\"QIA_X\", \"Qiagen N.v. (QIA_X)\"], \"QSC_X\": [\"QSC_X\", \"Qsc (QSC_X)\"], \"RAA_X\": [\"RAA_X\", \"Rational (RAA_X)\"], \"RHK_X\": [\"RHK_X\", \"Rh\u00c3\u2013n-klinikum (RHK_X)\"], \"RHM_X\": [\"RHM_X\", \"Rheinmetall (RHM_X)\"], \"RRTL_X\": [\"RRTL_X\", \"RTL S.A. (RRTL_X)\"], \"RWE_X\": [\"RWE_X\", \"Rwe  St (RWE_X)\"], \"S92_X\": [\"S92_X\", \"SMA Solar AG (S92_X)\"], \"SAP_X\": [\"SAP_X\", \"Sap (SAP_X)\"], \"SAX_X\": [\"SAX_X\", \"Str\u00c3\u00b6er SE & Co. KGaA (SAX_X)\"], \"SAZ_X\": [\"SAZ_X\", \"Stada Arzneimittel Ag (SAZ_X)\"], \"SBS_X\": [\"SBS_X\", \"Stratec Biomedical (SBS_X)\"], \"SDF_X\": [\"SDF_X\", \"K+s Aktiengesellschaft (SDF_X)\"], \"SFQ_X\": [\"SFQ_X\", \"SAF-Holland SA (SFQ_X)\"], \"SGL_X\": [\"SGL_X\", \"Sgl Carbon Se (SGL_X)\"], \"SIE_X\": [\"SIE_X\", \"Siemens (SIE_X)\"], \"SIX2_X\": [\"SIX2_X\", \"Sixt Se St (SIX2_X)\"], \"SKB_X\": [\"SKB_X\", \"Koenig & Bauer (SKB_X)\"], \"SKYD_X\": [\"SKYD_X\", \"Sky Deutschland (SKYD_X)\"], \"SLT_X\": [\"SLT_X\", \"Schaltbau Holding (SLT_X)\"], \"SOW_X\": [\"SOW_X\", \"Software (SOW_X)\"], \"SPR_X\": [\"SPR_X\", \"Axel Springer Se (SPR_X)\"], \"SRT3_X\": [\"SRT3_X\", \"Sartorius  Vz (SRT3_X)\"], \"SW1_X\": [\"SW1_X\", \"Shw (SW1_X)\"], \"SY1_X\": [\"SY1_X\", \"Symrise (SY1_X)\"], \"SZG_X\": [\"SZG_X\", \"Salzgitter (SZG_X)\"], \"SZU_X\": [\"SZU_X\", \"S\u00c3\u00bcdzucker (SZU_X)\"], \"TEG_X\": [\"TEG_X\", \"Tag Immobilien (TEG_X)\"], \"TIM_X\": [\"TIM_X\", \"ZEAL Network SE (TIM_X)\"], \"TKA_X\": [\"TKA_X\", \"Thyssenkrupp (TKA_X)\"], \"TLX_X\": [\"TLX_X\", \"Talanx Aktiengesellschaft (TLX_X)\"], \"TTI_X\": [\"TTI_X\", \"Tom Tailor Holding (TTI_X)\"], \"TTK_X\": [\"TTK_X\", \"Takkt (TTK_X)\"], \"TUI1_X\": [\"TUI1_X\", \"Tui (TUI1_X)\"], \"UTDI_X\": [\"UTDI_X\", \"United Internet (UTDI_X)\"], \"VIB3_X\": [\"VIB3_X\", \"Villeroy & Boch AG (VIB3_X)\"], \"VNA_X\": [\"VNA_X\", \"Vonovia SE (ex Deutsche Annington) (VNA_X)\"], \"VOS_X\": [\"VOS_X\", \"Vossloh (VOS_X)\"], \"VOW3_X\": [\"VOW3_X\", \"Volkswagen  Vz (VOW3_X)\"], \"VT9_X\": [\"VT9_X\", \"Vtg Aktiengesellschaft (VT9_X)\"], \"WAC_X\": [\"WAC_X\", \"Wacker Neuson Se (WAC_X)\"], \"WCH_X\": [\"WCH_X\", \"Wacker Chemie (WCH_X)\"], \"WDI_X\": [\"WDI_X\", \"Wirecard (WDI_X)\"], \"WIN_X\": [\"WIN_X\", \"Diebold Nixdorf AG (WIN_X)\"], \"ZIL2_X\": [\"ZIL2_X\", \"Elringklinger (ZIL2_X)\"], \"ZO1_X\": [\"ZO1_X\", \"Zooplus (ZO1_X)\"]}")
# Api-Key to access Quandl Web-Api
quandl.ApiConfig.api_key = 'tBctXCfFqZzRwSny_Znm'
# Web-Api by ECDC for COVID-19
infection_import_url = "https://opendata.ecdc.europa.eu/covid19/casedistribution/json/"
# Connect to HDFS via WebHdfsClient
hdfsconn = pywebhdfs.webhdfs.PyWebHdfsClient(base_uri_pattern=f"{hdfsweburl}/webhdfs/v1/",
                                         request_extra_opts={'verify': False, 'auth': ('admin', 'admin-password')})

# Base paths for the result data
infection_file_path = "/input/"
fse_file_path = "/input/"

# Create the base directory
hdfsconn.make_dir("/input", permission=777)

print("Starting separate Import Thread")
# Init the import thread once
import_thread = HDFSImportJob(hdfsconn, quandl_companies, fse_file_path, startdate, infection_import_url, infection_file_path, producer)
print("Import thread initialized. Main thread is going to sleep")
while True:
    #Sleep and let the thread do the work
    time.sleep(60)