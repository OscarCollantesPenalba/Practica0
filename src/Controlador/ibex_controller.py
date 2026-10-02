from src.modelo.spark_session import get_spark_session
from src.modelo.ibex_model import IbexModel
from src.vista.ibex_view import IbexView
from src.modelo.db_conexión import DBConnection


class IbexController:
    """Orquesta los ejercicios: pide datos al modelo y se los pasa a la vista."""

    def __init__(self):
        self.spark = get_spark_session()
        self.model = IbexModel(self.spark)
        self.view = IbexView()
        self.db = DBConnection()
        self.df = None  # DataFrame de trabajo, se va actualizando ejercicio a ejercicio

    def run(self):
        self.ej1a()
        self.ej1b()
        self.ej1c()
        self.ej2a()
        self.ej2b()
        self.ej3a()
        self.ej3b()
        self.ej4()
        self.ej5()
        self.ej6()
        self.guardar_bd()
        
    def ej1a(self):
        # Ej1-a
        self.view.titulo("Ej1-a")
        df = self.model.load_raw()
        self.view.esquema(df)
        self.df = self.model.convert_types(df)
        self.view.esquema(self.df)
        self.view.filas(self.df, 6)

    def ej1b(self):
        # Ej1-b
        self.view.titulo("Ej1-b")
        self.df = self.model.remove_suffix(self.df)
        self.view.filas(self.df, 6)

    def ej1c(self):
        # Ej1-c (opcional)
        self.view.titulo("Ej1-c")
        df_schema = self.model.load_with_schema()
        self.view.esquema(df_schema)
        self.view.filas(df_schema, 6)

    def ej2a(self):
        # Ej2-a
        self.view.titulo("Ej2-a")
        filas_antes = self.df.count()
        self.df = self.model.clean_data(self.df)
        filas_despues = self.df.count()
        self.view.valor("Filas eliminadas", filas_antes - filas_despues)
        self.view.valor("Empresas con información", len(self.df.columns) - 1)

    def ej2b(self):
        # Ej2-b
        self.view.titulo("Ej2-b")
        inicio, fin, dias = self.model.periodo(self.df)
        self.view.valor("Fecha inicial", inicio)
        self.view.valor("Fecha final", fin)
        self.view.valor("Días con información", dias)
        self.view.mensaje("Comentario: el periodo cubre todo 2024 con 255 sesiones, coherente con un año bursátil "
                          "(262 días laborables menos 7 sin datos, que parecen festivos de mercado).")
    
    def ej3a(self):
        # Ej3-a
        self.view.titulo("Ej3")
        self.df = self.model.rename_fecha(self.df)
        self.view.filas(self.df, 10)
        estadisticas = self.model.estadisticas_anuales(self.df)
        self.view.filas(estadisticas, estadisticas.count())
        df_deficiency = self.model.add_deficiency_notice(self.df)
        self.view.filas(df_deficiency, 100)
        
    def ej3b(self):
        #Ej3-b
        #Herramienta IA usda: Claude
        self.view.titulo("Ej3-b")
        self.view.mensaje("Comentario/Reflexión/Investigación: las empresas son las que formaron parte del IBEX 35 en 2024; "
                        "el Comité Asesor Técnico de BME las elige sobre todo por liquidez (volumen negociado en euros) "
                        "y por superar una capitalización mínima, no por su tamaño.")
        self.view.mensaje("Los NULL de MEL y PUIG vienen del cambio de composición del índice: Puig sustituyó a Meliá "
                        "el 22/07/2024, y cada una solo tiene datos mientras estuvo en el IBEX 35.")
        self.view.mensaje("Afectan solo al Ej3: avg, max y min ignoran los NULL, así que MEL y PUIG se calculan con "
                        "141 y 114 días de 255, no con todo el año.")
        self.view.mensaje("Referencia/s: CNMV, infografía del IBEX 35 (https://www.cnmv.es/DocPortal/Publicaciones/Infografias/IBEX35.pdf); "
                        "Valencia Plaza, 'Puig entrará en el Ibex 35 a partir del 22 de julio', julio de 2024 "
                        "(https://valenciaplaza.com/puig-entrara-ibex-35-partir-22-julio); "
                        "INEAF, 'Qué es el IBEX 35' (https://www.ineaf.es/tribuna/que-es-ibex-35/).")
        
    def ej4(self):
        # Ej4
        # Herramienta IA usada: Claude
        self.view.titulo("Ej4")
        variaciones = self.model.variacion_anual(self.df)
        self.view.filas(variaciones, variaciones.count())
        
    def ej5(self):
        #Ej5
        self.view.titulo("Ej5")
        df_cuartiles = self.model.add_cuartiles(self.df)
        self.view.filas(df_cuartiles,1)
        aena_bbva = df_cuartiles.select("Dia", "AENA", "AENACuartil", "BBVA", "BBVACuartil")
        self.view.filas(aena_bbva, aena_bbva.count())
    
    def ej6(self):
        # Ej6 (opcional)
        ##Herramienta IA usda: Claude
        self.view.titulo("Ej6")
        # No se guarda en self.df: la tabla final de la BD va "sin nuevas columnas"
        df_cambios = self.model.add_cambio_significativo(self.df)
        self.view.filas(self.model.fila(df_cambios, 15), 1)
        self.view.mensaje("Comentario/Reflexión/Investigación: hay 29 variaciones diarias superiores al 8 % (menos del 0,5 % de las "
                          "observaciones) en 12 empresas, y 15 son de Grifols. Las causas que se han contrastado son noticias propias "
                          "de cada empresa:")
        self.view.mensaje("- Grifols: el 09/01 (-25,9 %) el fondo bajista Gotham City Research la acusó de manipular su deuda y su EBITDA; "
                          "el 29/02 (-35,0 %) publicó las cuentas de 2023 sin la firma de su auditor KPMG, y el 08/03 (+19,8 %) rebotó "
                          "cuando KPMG las aprobó 'sin salvedades'; el 08/07 (+9,7 %) subió por la posible OPA de exclusión de Brookfield "
                          "y el 27/11 (-9,1 %) cayó al abandonar Brookfield esa OPA.")
        self.view.mensaje("- Naturgy: el 11/06 (-15,0 %) cayó porque CriteriaCaixa y Taqa dieron por rotas las negociaciones para una OPA. "
                          "Solaria: el 14/06 (+9,8 %) subió por una información de Bloomberg sobre ofertas de compra no solicitadas.")
        self.view.mensaje("- Puig: el 06/09 (-13,7 %) cayó con sus primeros resultados como cotizada (beneficio semestral -27 %, "
                          "por costes de la salida a bolsa). Rovi: el 07/11 (-13,1 %) cayó tras sus resultados del tercer trimestre "
                          "y la cancelación del lanzamiento de Risvan en EE. UU.")
        self.view.mensaje("- Acciona (ANA): el 06/11 (-8,1 %) cayó por la victoria de Trump, que perjudica a las renovables.")
        self.view.mensaje("Interpretación: las variaciones extremas no son ruido, son reacciones a hechos concretos: ataques de bajistas y "
                          "dudas contables, operaciones corporativas (OPA), resultados y política. Se concentran en pocos valores, sobre "
                          "todo Grifols, que acumula volatilidad por su incertidumbre, y casi todas afectan a una sola empresa, no al "
                          "índice entero.")
        self.view.mensaje("Referencia/s: El Independiente, 'Grifols se desploma casi un 26 %...' (https://www.elindependiente.com/economia/2024/01/09/grifols-desploma-bolsa-falsear-cuentas/amp/); "
                          "Forbes España, 'Grifols firma el peor resultado en Bolsa...' (https://forbes.es/ultima-hora/420817/grifols-firma-el-peor-resultado-en-bolsa-al-caer-un-10-tras-el-nuevo-ataque-de-los-bajistas/); "
                          "The Objective, 'Grifols rebota un 19 % tras el visto bueno de KPMG' (https://theobjective.com/economia/2024-03-08/grifols-rebota-kpmg-bolsa/); "
                          "El Independiente, 'Annus horribilis Grifols' (https://www.elindependiente.com/economia/2024/07/09/annus-horribilis-grifols-informe-gotham-posible-opa-exclusion/amp/); "
                          "El Observador, 'Brookfield abandona la OPA y Grifols se desploma' (https://www.elobservador.com.uy/espana/empresas-y-negocios/brookfield-abandona-la-opa-y-grifols-se-desploma-el-ibex-n5972264); "
                          "El Independiente, 'Naturgy se desploma un 15 %...' (https://www.elindependiente.com/economia/2024/06/11/naturgy-se-desploma-mas-de-un-12-en-el-ibex-tras-romper-las-negociaciones-con-taqa/); "
                          "Público, 'Solaria se dispara en Bolsa ante una posible OPA' (https://www.publico.es/economia/solaria-dispara-bolsa-posible-opa.html); "
                          "Benzinga, 'Puig sorprende con caída del 27 % en beneficios' (https://es.benzinga.com/news/spain/stocks-spain-news/puig-sorprende-con-caida-del-27-en-beneficios-a-pesar-del-aumento-en-ventas); "
                          "XTB, 'Las acciones de Rovi caen tras sus resultados' (https://www.xtb.com/es/analisis-de-mercado/las-acciones-de-rovi-caen-tras-sus-resultados); "
                          "Bolsamania, 'Fuertes pérdidas para BBVA, Sabadell, Acciona y Solaria tras la victoria de Trump', 06/11/2024 "
                          "(https://www.bolsamania.com/catalunya/noticies_print/empreses/fortes-perdues-per-a-bbva-sabadell-acciona-i-solaria-despres-de-la-victoria-de-trump--17937892.html).")

#========================================================================================
#===================Guardar en Base de Datos ============================================
#========================================================================================

    def guardar_bd(self):
        # Almacenamiento en la base de datos IBEX35 (tabla Datos2024)
        # Herramienta IA usada: Claude
        self.view.titulo("BD")
        try:
            # 1) Datos completos tal cual vienen del CSV
            self.db.write_table(self.model.load_raw(), "Datos2024")
            self.view.mensaje("Datos completos del CSV guardados en la tabla Datos2024")
            # 2) Datos tratados (self.df no incluye las columnas nuevas de Ej3, Ej5 y Ej6)
            self.db.write_table(self.df, "Datos2024")
            self.view.mensaje("Datos tratados guardados en la tabla Datos2024")
        except Exception as error:
            # Si no hay MySQL, la base IBEX35 o las credenciales (README), el resto de ejercicios sigue valiendo
            # Py4J pone la causa real (p. ej. "Access denied", "Unknown database") en la 2.ª línea
            lineas = str(error).splitlines()
            causa = lineas[1] if len(lineas) > 1 else lineas[0]
            self.view.mensaje(f"No se pudo guardar en la base de datos (revisar README): {causa.lstrip(': ')}")
            