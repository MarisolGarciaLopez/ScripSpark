#PRACTICA SPARK
#JOHANA MARISOL GARCIA LOPEZ 
#CARGO LIBRERIAS
pacman::p_load(httr, tidyverse, leaflet, janitor, readr, sparklyr, xlsx, XML) 


url <- "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
httr::GET(url)

sc <- spark_connect(master = "local") #En mi caso me da error (desde el primer día) por el core#

#Ejercicio A.i. Limpieza de datos del Dataset
ds <- jsonlite::fromJSON(url)
ds <- ds$ListaEESSPrecio
ds <- ds %>% as_tibble() %>% clean_names()
View(ds)
ds <- ds %>% type_convert(locale = locale(decimal_mark = ",")) %>% view() %>% clean_names()

#Ejercicio A.ii. Generar informe y explique si encuentra alguna anomalía del punto anteiror. 
#Este apartado se encuentra detallado en la memoria descriptiva. 


#Ejercicio A.iii. Nueva columna llamada low-cost clasificada por low-cost y no low-cost. Y precio promedio
#de todos los combustible.

ds_columnaLowCost <- ds %>% mutate(low_cost=rotulo%in%c("REPSOL","CAMPSA","BP", "SHELL","GALP", "CEPSA")) %>% 
  filter(low_cost==low_cost) %>% 
  group_by(provincia) %>% view()

ds_columnaLowCost$low_cost[ds_columnaLowCost$low_cost == "FALSE"] <- "low cost"
ds_columnaLowCost$low_cost[ds_columnaLowCost$low_cost == "TRUE"] <- "No low cost"

View(ds_columnaLowCost)
#FALSE es igual a low cost
#TRUE es igual NO low cost 
#Ahora hago la media 

precio_promedio_CCAA <- ds_columnaLowCost %>% select(rotulo, idccaa, provincia, precio_biodiesel, precio_bioetanol, precio_gas_natural_comprimido, precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno) %>% 
  group_by(idccaa) %>% summarise(mean(precio_biodiesel, na.rm=TRUE), mean(precio_bioetanol, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e10, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE))
  

View(precio_promedio_CCAA)

precio_promedio_provincias <- ds_columnaLowCost %>% select(rotulo, idccaa, provincia, precio_biodiesel, precio_bioetanol, precio_gas_natural_comprimido, precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno) %>% 
  group_by(idccaa, provincia) %>% summarise(mean(precio_biodiesel, na.rm=TRUE), mean(precio_bioetanol, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e10, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE))

View(precio_promedio_provincias)


#EJERCICIO A iv.Crear mapa interactivo de 

#top10 mas caras
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5_premium, localidad, direccion) %>% 
  top_n(10, precio_gasolina_95_e5_premium) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5_premium)

#top20 mas baratas
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5_premium, localidad, direccion) %>% 
  top_n(-20, precio_gasolina_95_e5_premium) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5_premium)


#EJERCICIO A v. Guardar archivo en formato CSV

write.csv(ds_columnaLowCost,"C:/Users/jmari/OneDrive/Escritorio/spark/low-cost_u_22138972.csv", row.names = FALSE)


#EJERCICIO B i. 

numero_madrid <- ds_columnaLowCost %>% select(idccaa, low_cost, provincia) %>% group_by(idccaa, provincia) %>% 
  filter(idccaa== 13) %>% count(low_cost)

View(numero_madrid)

numero_cataluña <- ds_columnaLowCost %>% select(idccaa, low_cost, provincia) %>% group_by(idccaa, provincia) %>% 
  filter(idccaa== "09") %>% count(low_cost)


View(numero_cataluña)


#Ejercicio B ii.Precio promedio, Mas bajo y mas caro de gasoleo a y gasolina 95 e5 Premium

madrid <- ds_columnaLowCost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% group_by(idccaa) %>% 
  filter(idccaa== 13) %>% summarise(mean(precio_gasoleo_a, na.rm = TRUE), mean(precio_gasolina_95_e5_premium, na.rm = TRUE), max(precio_gasoleo_a, na.rm = TRUE), max(precio_gasolina_95_e5_premium, na.rm = TRUE), min(precio_gasoleo_a, na.rm = TRUE), min(precio_gasolina_95_e5_premium, na.rm = TRUE))


View(madrid)


barcelona <- ds_columnaLowCost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% group_by(idccaa) %>% 
  filter(idccaa== "09") %>% summarise(mean(precio_gasoleo_a, na.rm = TRUE), mean(precio_gasolina_95_e5_premium, na.rm = TRUE), max(precio_gasoleo_a, na.rm = TRUE), max(precio_gasolina_95_e5_premium, na.rm = TRUE), min(precio_gasoleo_a, na.rm = TRUE), min(precio_gasolina_95_e5_premium, na.rm = TRUE))
  
View(barcelona)

#Ejercicio B iii. Guardar archivo en formato CSV 

write.csv(madrid,"C:/Users/jmari/OneDrive/Escritorio/spark/informe_MAD_u_22138972.csv", row.names = FALSE)
write.csv(barcelona,"C:/Users/jmari/OneDrive/Escritorio/spark/informe_BCN_u_22138972.csv", row.names = FALSE)


#Ejercicio C i. A nivel municipios, cuantas gasolineras son low cost y No low cost con precio promedio, mas bajo y mas caro. Exceptuando grandes ciudades Españolas.

no_grandes_ciudades <- ds_columnaLowCost %>% select(idccaa, id_municipio, municipio, low_cost, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% group_by(municipio, low_cost) %>% filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))

View(no_grandes_ciudades)
write.csv(no_grandes_ciudades,"C:/Users/jmari/OneDrive/Escritorio/spark/informe_no_grandes_ciudades_u22138972.csv", row.names = FALSE)

cantidad_grandes_ciudades <- no_grandes_ciudades %>% group_by(low_cost) %>% count(low_cost)
View(cantidad_grandes_ciudades)
write.csv(no_grandes_ciudades,"C:/Users/jmari/OneDrive/Escritorio/spark/informe_no_grandes_ciudades_cantidad_u22138972.csv", row.names = FALSE)


#Ejercicio D i. Gasolineras que se encuentran abiertas 24 H exclusivamente 

no_24_horas <- ds_columnaLowCost %>% select(rotulo, provincia, horario, direccion) %>% group_by(provincia) %>% filter(horario == "L-D: 24H") %>%  select(rotulo, provincia, !horario, direccion)
View(no_24_horas)


#Ejercicio D ii. Guardar archivo en formato excel 
library(XML)
library(xlsx)

write.csv(no_24_horas,"C:/Users/jmari/OneDrive/Escritorio/spark/no_24_horas_u22138972.csv", row.names = FALSE)


#Ejercicio e i. Añadir población al dataset original creando colummna denominada población 

library(spatial)
library(readr)
pobmun21 <- read_csv("pobmun21.csv")
View(pobmun21)

names(pobmun21)

poblacion <- rename(pobmun21, id_provincia = "CPRO", provincia = "PROVINCIA", cmun = "CMUN", municipio = "NOMBRE")
View(poblacion)

union <-left_join(ds, poblacion, "municipio")
View(union)

#Ejercicio eii. Calcule competencia con gasolinera y direccion 
union %>%  select(rotulo, latitud, longitud_wgs84, municipio, localidad, direccion) %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~municipio) %>% 
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 1000)

union %>%  select(rotulo, latitud, longitud_wgs84, municipio, localidad, direccion) %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~municipio) %>% 
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 2000)

union %>%  select(rotulo, latitud, longitud_wgs84, municipio, localidad, direccion) %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~municipio) %>% 
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 4000)


#Ejercicio eiii. Top ten de municipios excepto territorio insular donde don existan gasolineras 24H agrupadas como low cost y no low cost 

informe_top_ten <- ds_columnaLowCost %>% filter(!provincia %in% c("BALEARS (ILLES)","PALMAS (LAS)")) %>% 
  filter(!horario == "L-D: 24H") %>% group_by(municipio, low_cost) %>% count() 

View(informe_top_ten)

write.csv(no_24_horas,"C:/Users/jmari/OneDrive/Escritorio/spark/informe_top_ten_u22138972.csv", row.names = FALSE)









