package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

// =====================
// Структуры для подсчета статистики
// =====================

type ClientStats struct {
	TotalRequests int         `json:"total_requests"`
	StatusCounts  map[int]int `json:"status_counts"`
	Positive      int         `json:"positive"`
	Negative      int         `json:"negative"`
}

type Stats struct {
	TotalRequests int                    `json:"total_requests"`
	StatusCounts  map[int]int            `json:"status_counts"`
	Positive      int                    `json:"positive"`
	Negative      int                    `json:"negative"`
	RateLimited   int                    `json:"rate_limited"`
	ClientStats   map[string]ClientStats `json:"client_stats"`
}

var (
	serverStats = Stats{
		StatusCounts: make(map[int]int),
		ClientStats:  make(map[string]ClientStats),
	}
	statsMutex sync.Mutex
)

// =====================
// Реализация сервера
// =====================

var serverLimiter chan struct{} // Ограничение пропускной способности сервера (5 запросов/сек)

func postHandler(w http.ResponseWriter, r *http.Request) {
	// Если лимит запросов исчерпан – возвращаем 429 и обновляем статистику
	select {
	case <-serverLimiter:
		// Токен получен – продолжаем обработку запроса.
	default:
		w.WriteHeader(http.StatusTooManyRequests) // 429
		w.Write([]byte("Server rate limit exceeded"))
		statsMutex.Lock()
		serverStats.TotalRequests++
		serverStats.StatusCounts[http.StatusTooManyRequests]++
		serverStats.RateLimited++
		statsMutex.Unlock()
		return
	}

	// Симуляция ответа: 70% положительных (200 или 202) и 30% отрицательных (400 или 500).
	randVal := rand.Intn(100)
	var status int
	var isPositive bool
	if randVal < 70 {
		isPositive = true
		if rand.Intn(2) == 0 {
			status = http.StatusOK // 200
		} else {
			status = http.StatusAccepted // 202
		}
	} else {
		isPositive = false
		if rand.Intn(2) == 0 {
			status = http.StatusBadRequest // 400
		} else {
			status = http.StatusInternalServerError // 500
		}
	}

	// Обновление статистики сервера
	statsMutex.Lock()
	serverStats.TotalRequests++
	serverStats.StatusCounts[status]++
	if isPositive {
		serverStats.Positive++
	} else {
		serverStats.Negative++
	}

	// Обновление статистики по клиенту (если указан заголовок X-Client-ID)
	clientID := r.Header.Get("X-Client-ID")
	if clientID != "" {
		// Если клиента еще нет в статистике, создаем его запись
		if _, exists := serverStats.ClientStats[clientID]; !exists {
			serverStats.ClientStats[clientID] = ClientStats{StatusCounts: make(map[int]int)}
		}

		// Обновляем статистику клиента
		cs := serverStats.ClientStats[clientID]
		cs.TotalRequests++
		cs.StatusCounts[status]++
		if isPositive {
			cs.Positive++
		} else {
			cs.Negative++
		}
		serverStats.ClientStats[clientID] = cs // Записываем обновленные данные обратно в карту
	}

	statsMutex.Unlock()

	w.WriteHeader(status)
	w.Write([]byte(fmt.Sprintf("Response with status %d", status)))
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	// Если путь /stats – возвращаем JSON со статистикой,
	// иначе – простой ответ для проверки доступности (client3).
	if r.URL.Path == "/stats" {
		statsMutex.Lock()
		statsData, err := json.MarshalIndent(serverStats, "", "  ")
		statsMutex.Unlock()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error marshalling stats"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(statsData)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Server is up"))
}

// =====================
// Реализация клиентов
// =====================

var serverPort string // номер порта сервера (из env)

func runClientPost(clientID string, totalRequests int, clientRateLimit int, wg *sync.WaitGroup) {
	// Ограничение количества исходящих запросов: 5 req/sec
	ticker := time.NewTicker(time.Second / time.Duration(clientRateLimit))
	defer ticker.Stop()

	var clientStats ClientStats
	clientStats.StatusCounts = make(map[int]int)
	var mu sync.Mutex

	// Функция-воркер: за один цикл отправляет 5 POST запросов.
	worker := func(workerID int, iterations int, workerWg *sync.WaitGroup) {
		defer workerWg.Done()
		for i := 0; i < iterations; i++ {
			for j := 0; j < 5; j++ {
				<-ticker.C // ожидание, чтобы не превышать лимит
				req, err := http.NewRequest(http.MethodPost, "http://localhost:"+serverPort, nil)
				if err != nil {
					log.Println("Error creating request:", err)
					continue
				}
				req.Header.Set("X-Client-ID", clientID)
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Println("Error sending request:", err)
					continue
				}

				mu.Lock()
				clientStats.TotalRequests++
				clientStats.StatusCounts[resp.StatusCode]++
				// Для клиента считаем 200 и 202 как положительные, остальные – отрицательные
				if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
					clientStats.Positive++
				} else if resp.StatusCode == http.StatusBadRequest ||
					resp.StatusCode == http.StatusInternalServerError ||
					resp.StatusCode == http.StatusTooManyRequests {
					clientStats.Negative++
				}
				mu.Unlock()
				resp.Body.Close()
			}
		}
	}

	// Для totalRequests запросов, при 2 воркерах по 5 запросов за цикл
	iterations := totalRequests / (2 * 5)
	var workerWg sync.WaitGroup
	workerWg.Add(2)
	go worker(1, iterations, &workerWg)
	go worker(2, iterations, &workerWg)
	workerWg.Wait()

	// Вывод статистики для клиента
	fmt.Printf("Client %s finished sending requests.\n", clientID)
	fmt.Printf("Отправлено запросов: %d\n", clientStats.TotalRequests)
	fmt.Printf("Разбивка по статусам: ")
	for code, count := range clientStats.StatusCounts {
		fmt.Printf("%d - %d, ", code, count)
	}
	fmt.Println()
	fmt.Printf("Положительных: %d, Отрицательных: %d\n", clientStats.Positive, clientStats.Negative)
	wg.Done()
}

func runClientStatusChecker(workerID string, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	// Каждые 5 секунд проверяем, доступен ли сервер
	for {
		select {
		case <-done:
			fmt.Printf("%s stopping status checks\n", workerID)
			return
		case <-time.After(5 * time.Second):
			resp, err := http.Get("http://localhost:" + serverPort)
			if err != nil {
				fmt.Printf("%s: Server is down\n", workerID)
			} else {
				fmt.Printf("%s: Server is up, status: %s\n", workerID, resp.Status)
				resp.Body.Close()
			}
		}
	}
}

// =====================
// main – запуск сервера и клиентов
// =====================
func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Нет файла .env, по умолчанию 8080")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	serverPort = port

	rand.Seed(time.Now().UnixNano())

	// Серверный лимитер (5 запросов/сек)
	serverLimiter = make(chan struct{}, 5)
	ticker := time.NewTicker(200 * time.Millisecond)
	go func() {
		for range ticker.C {
			select {
			case serverLimiter <- struct{}{}:
			default:
				// если токенов уже 5 – ничего не делаем
			}
		}
	}()

	// обработчики
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			getHandler(w, r)
		} else if r.Method == http.MethodPost {
			postHandler(w, r)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("Method not allowed"))
		}
	})

	// слушатель для запуска сервера
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	// Запуск сервера
	log.Printf("Server starting on port %s\n", port)
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Далее запускаем клиентов и статус-чекеры
	var postClientsWg sync.WaitGroup
	postClientsWg.Add(2)
	go runClientPost("client1", 100, 5, &postClientsWg)
	// time.Sleep(3 * time.Second)
	go runClientPost("client2", 100, 5, &postClientsWg)

	var statusWg sync.WaitGroup
	statusWg.Add(2)
	doneChan := make(chan struct{})
	go runClientStatusChecker("client3_worker1", doneChan, &statusWg)
	go runClientStatusChecker("client3_worker2", doneChan, &statusWg)

	postClientsWg.Wait()
	close(doneChan)
	statusWg.Wait()

	// Получение статистики с сервера.
	resp, err := http.Get("http://localhost:" + port + "/stats")
	if err != nil {
		log.Println("Error fetching server stats:", err)
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("\nServer Stats (JSON):")
		fmt.Println(string(body))
		resp.Body.Close()
	}
}
