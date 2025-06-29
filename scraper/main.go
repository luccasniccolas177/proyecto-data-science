package main

import (
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Propiedad (estructura sin cambios)
type Propiedad struct {
	Valor                    float64
	ValorUF                  float64
	NumeroDeHabitaciones     int
	NumeroDeBanos            int
	NumeroDeEstacionamientos int
	SuperficieTotal          float64
	SuperficieConstruida     float64
	FechaConstruccion        int
	Comuna                   string
	Direccion                string
	URL                      string
	TipoVivienda             string
	QuienVende               string
	Corredor                 string
	Latitud                  float64
	Longitud                 float64
	Descripcion              string
}

func (p Propiedad) PrintInfo() { /* ... sin cambios ... */ }
func extractLatLngFromScript(scriptContent string) (float64, float64) {
	re := regexp.MustCompile(`var publicationLocation\s*=\s*\[\s*(-?[0-9]+\.[0-9]+)\s*,\s*(-?[0-9]+\.[0-9]+)\s*\];`)
	matches := re.FindStringSubmatch(scriptContent)
	if len(matches) == 3 {
		lat, errLat := strconv.ParseFloat(matches[1], 64)
		lon, errLon := strconv.ParseFloat(matches[2], 64)
		if errLat == nil && errLon == nil {
			return lat, lon
		}
	}
	return 0.0, 0.0
}
func CleanMoney(m string) (float64, error) {
	parts := strings.Fields(m)
	if len(parts) < 2 {
		return 0, fmt.Errorf("formato monetario inesperado: %s", m)
	}
	strMoney := parts[1]
	strMoney = strings.ReplaceAll(strMoney, ".", "")
	strMoney = strings.ReplaceAll(strMoney, ",", ".")
	return strconv.ParseFloat(strMoney, 64)
}
func CleanIntegers(n string) (int, error) {
	strNum := strings.Fields(n)[0]
	strNum = strings.ReplaceAll(strNum, ".", "")
	return strconv.Atoi(strNum)
}
func CleanArea(a string) (float64, error) {
	strArea := strings.Fields(a)[0]
	strArea = strings.ReplaceAll(strArea, ".", "")
	strArea = strings.ReplaceAll(strArea, ",", ".")
	return strconv.ParseFloat(strArea, 64)
}

func main() {
	c := colly.NewCollector(
		colly.Async(true),
	)

	// <<< AJUSTE DE ESTABILIDAD: Configuración menos agresiva
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 6,               // Reducimos los hilos de 12 a 6
		RandomDelay: 2 * time.Second, // Aumentamos el delay aleatorio hasta 2 segundos
	})
	// <<< AJUSTE DE ESTABILIDAD: Añadimos un timeout a las peticiones
	c.SetRequestTimeout(30 * time.Second)

	collectorPropiedades := c.Clone()

	var propiedades []Propiedad
	var pMutex = &sync.Mutex{}
	visitedProperties := make(map[string]bool)
	var vMutex = &sync.Mutex{}
	var propiedadesCount int

	collectorPropiedades.OnRequest(func(r *colly.Request) {})

	collectorPropiedades.OnHTML("body", func(e *colly.HTMLElement) {
		var currentLabel string
		currentProperty := Propiedad{}
		currentProperty.URL = e.Request.URL.String()

		e.DOM.Find(".clp-details-table").Each(func(i int, s *goquery.Selection) {
			s.Children().Each(func(i int, s *goquery.Selection) {
				if s.HasClass("clp-description-label") {
					currentLabel = strings.TrimSpace(s.Text())
				} else if s.HasClass("clp-description-value") {
					value := strings.TrimSpace(s.Text())
					var err error
					switch currentLabel {
					case "Valor:":
						if strings.HasPrefix(value, "$") {
							currentProperty.Valor, err = CleanMoney(value)
						} else if strings.HasPrefix(value, "UF") {
							currentProperty.ValorUF, err = CleanMoney(value)
						}
					case "Valor (UF aprox.)*:":
						currentProperty.ValorUF, err = CleanMoney(value)
					case "Valor (CLP aprox.)*:":
						currentProperty.Valor, err = CleanMoney(value)
					case "Habitaciones:":
						currentProperty.NumeroDeHabitaciones, err = CleanIntegers(value)
					case "Baño:", "Baños:":
						currentProperty.NumeroDeBanos, err = CleanIntegers(value)
					case "Estacionamientos:":
						currentProperty.NumeroDeEstacionamientos, err = CleanIntegers(value)
					case "Superficie Total:":
						currentProperty.SuperficieTotal, err = CleanArea(value)
					case "Superficie Construida:":
						currentProperty.SuperficieConstruida, err = CleanArea(value)
					case "Año Construcción:":
						currentProperty.FechaConstruccion, err = CleanIntegers(value)
					case "Dirección:":
						direccion := strings.Split(value, ",")
						currentProperty.Comuna = strings.TrimSpace(direccion[0])
						if len(direccion) > 1 {
							currentProperty.Direccion = strings.TrimSpace(strings.Join(direccion[1:], ","))
						}
					case "Tipo de propiedad:":
						currentProperty.TipoVivienda = value
					}
					if err != nil {
						log.Printf("Error de parseo en URL %s, campo '%s', valor '%s': %v", e.Request.URL, currentLabel, value, err)
					}
				}
			})
		})

		e.DOM.Find(".clp-publication-contact-box").Each(func(_ int, box *goquery.Selection) {
			box.Find("h2.subtitle").Each(func(_ int, h2 *goquery.Selection) {
				h2Text := strings.TrimSpace(strings.ToLower(h2.Text()))
				table := h2.Next()
				if h2Text == "información de contacto" {
					nombre := strings.TrimSpace(table.Find("tr").First().Find("td").Text())
					if nombre != "" {
						currentProperty.QuienVende = nombre
					}
				} else if h2Text == "corredora" {
					corredora := strings.TrimSpace(table.Find("tr").First().Find("td").Text())
					if corredora != "" {
						currentProperty.Corredor = corredora
					}
				}
			})
			if currentProperty.Corredor == "" {
				currentProperty.Corredor = "Dueño Directo"
			}
		})

		e.ForEach("script", func(_ int, el *colly.HTMLElement) {
			if strings.Contains(el.Text, "publicationLocation") {
				lat, lon := extractLatLngFromScript(el.Text)
				currentProperty.Latitud = lat
				currentProperty.Longitud = lon
			}
		})

		currentProperty.Descripcion = strings.TrimSpace(e.DOM.Find("div.clp-description-box").Text())

		pMutex.Lock()
		propiedades = append(propiedades, currentProperty)
		propiedadesCount = len(propiedades)
		log.Printf("Propiedad %d: %s", propiedadesCount, currentProperty.URL)
		pMutex.Unlock()
	})

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		if e.DOM.HasClass("page-link") {
			link := e.Request.AbsoluteURL(e.Attr("href"))
			c.Visit(link)
		}
		if e.Attr("class") == "d-block text-ellipsis clp-big-value" {
			link := e.Request.AbsoluteURL(e.Attr("href"))
			vMutex.Lock()
			if !visitedProperties[link] {
				visitedProperties[link] = true
				vMutex.Unlock()
				collectorPropiedades.Visit(link)
			} else {
				vMutex.Unlock()
			}
		}
	})

	c.OnRequest(func(r *colly.Request) {
		log.Println("Visitando:", r.URL.String())
	})

	// <<< AJUSTE DE ESTABILIDAD: Manejo de errores de red
	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Error en la petición a %s: %v", r.Request.URL, err)
		// Opcional: podrías reintentar la visita
		// r.Request.Retry()
	})

	startURL := "https://chilepropiedades.cl/propiedades/venta/casa/region-metropolitana-de-santiago-rm/0"
	log.Println("Iniciando scraping en:", startURL)
	c.Visit(startURL)

	c.Wait()
	collectorPropiedades.Wait()

	log.Printf("\nScraping finalizado. Se procesaron %d propiedades.\n", len(propiedades))

	file, err := os.Create("propiedades.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	headers := []string{
		"Comuna", "Link", "Tipo_Vivienda", "N_Habitaciones", "N_Baños", "N_Estacionamientos",
		"Total_Superficie", "Superficie_Construida", "Valor_UF", "Valor_CLP",
		"Dirección", "Quien_Vende", "Corredor", "Latitud", "Longitud", "Descripcion",
	}
	if err := w.Write(headers); err != nil {
		log.Fatal("error writing headers to file", err)
	}

	for _, p := range propiedades {
		row := []string{
			p.Comuna, p.URL, p.TipoVivienda,
			strconv.Itoa(p.NumeroDeHabitaciones), strconv.Itoa(p.NumeroDeBanos),
			strconv.Itoa(p.NumeroDeEstacionamientos), fmt.Sprintf("%.2f", p.SuperficieTotal),
			fmt.Sprintf("%.2f", p.SuperficieConstruida), fmt.Sprintf("%.2f", p.ValorUF),
			fmt.Sprintf("%.0f", p.Valor), p.Direccion,
			p.QuienVende, p.Corredor,
			fmt.Sprintf("%f", p.Latitud), fmt.Sprintf("%f", p.Longitud),
			p.Descripcion,
		}
		if err := w.Write(row); err != nil {
			log.Fatal("error writing record to file", err)
		}
	}

	log.Println("Datos guardados exitosamente en propiedades.csv")
}
