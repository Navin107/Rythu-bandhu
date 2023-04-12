from configparser import ConfigParser


config = ConfigParser(interpolation=None)

config["server_setup"] = {
    
    "username" :"admin",
    "password" :"GrZ4y(5Zc2K9N%j3SX5v7d2!WmJ",
    "host" :"databroker.iudx.io",
    "port" :24567,
    "vhost":"ADeX-INTERNAL"}

config["ppp_contact_details_queue"] = {
    "queue": "rpc-adapter-reply-queue"
}

config["ppp_contact_details_url"] ={
    "url" : "http://rythubandhu.telangana.gov.in/RB_ADEX_IISC.asmx?op=Get_PPB_ContactDtls"
}

config["master_data_url"] ={
    "url" : "http://rythubandhu.telangana.gov.in/RB_ADEX_IISC.asmx?op=Get_RB_Master_Data"
}

config["cb_data_url"] ={
    "url" : "http://rythubandhu.telangana.gov.in/RB_ADEX_IISC.asmx?op=Get_CB_Data"
}

config["iudx_credentials"] = {
    "username": "IISC",
    "password": "II$SC@0404"
}
with open("config_file.ini", "w") as f:
    config.write(f)
