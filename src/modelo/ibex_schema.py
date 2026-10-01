from pyspark.sql.types import StructType, StructField, DateType, DecimalType

EMPRESAS = {
    "IBE.MC": "Iberdrola",      "REP.MC": "Repsol",        "NTGY.MC": "Naturgy",
    "ELE.MC": "Endesa",         "ENG.MC": "Enagas",        "RED.MC": "Redeia",
    "SAN.MC": "Santander",      "BBVA.MC": "BBVA",         "CABK.MC": "CaixaBank",
    "BKT.MC": "Bankinter",      "SAB.MC": "Sabadell",      "UNI.MC": "Unicaja",
    "MAP.MC": "Mapfre",         "ACS.MC": "ACS",           "ANA.MC": "Acciona",
    "ANE.MC": "AccionaEnergia", "ACX.MC": "Acerinox",      "MTS.MC": "ArcelorMittal",
    "SCYR.MC": "Sacyr",         "CLNX.MC": "Cellnex",      "TEF.MC": "Telefonica",
    "AENA.MC": "AENA",          "FER.MC": "Ferrovial",     "ITX.MC": "Inditex",
    "AMS.MC": "Amadeus",        "IAG.MC": "IAG",           "GRF.MC": "Grifols",
    "FDR.MC": "Fluidra",        "SLR.MC": "Solaria",       "ROVI.MC": "Rovi",
    "LOG.MC": "Logista",        "IDR.MC": "Indra",         "MEL.MC": "Melia",
    "PUIG.MC": "Puig",          "COL.MC": "Colonial",      "MRL.MC": "Merlin",
}


def build_schema():
    """Fecha como DateType y una columna DecimalType(10, 2) por empresa."""
    campos = [StructField("Fecha", DateType(), True)]
    campos += [StructField(nombre, DecimalType(10, 2), True) for nombre in EMPRESAS.values()]
    return StructType(campos)