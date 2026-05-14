package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the application.
// Values are loaded once at startup from config/.env (or real env vars in prod).
type Config struct {
	// Server
	APIPort string
	Env     string

	// MySQL
	DBHost            string
	DBPort            string
	DBUser            string
	DBPassword        string
	DBName            string
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisPoolSize int
	RedisURLTTL   time.Duration

	// Kafka
	KafkaBrokers         []string
	KafkaTopicClickEvents string
	KafkaConsumerGroup   string
	KafkaProducerRetries int

	// App
	ShortCodeLength int
	BaseURL         string
}

// Load reads the .env file and returns a populated Config.
// Call this once in main() and pass the result everywhere via dependency injection.
func Load(envFile string) (*Config, error) {
	if err := godotenv.Load(envFile); err != nil {
		log.Printf("[config] no .env file found at %s, falling back to OS env", envFile)
	}

	cfg := &Config{
		APIPort: getEnv("API_PORT", "8081"),
		Env:     getEnv("ENV", "development"),

		DBHost:            getEnv("DB_HOST", "localhost"),
		DBPort:            getEnv("DB_PORT", "3306"),
		DBUser:            getEnv("DB_USER", "appuser"),
		DBPassword:        getEnv("DB_PASSWORD", "apppass"),
		DBName:            getEnv("DB_NAME", "urlshortener"),
		DBMaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
		DBMaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 10),
		DBConnMaxLifetime: getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),
		RedisPoolSize: getEnvInt("REDIS_POOL_SIZE", 10),
		RedisURLTTL:   getEnvDuration("REDIS_URL_TTL", time.Hour),

		KafkaBrokers:          []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopicClickEvents: getEnv("KAFKA_TOPIC_CLICK_EVENTS", "click_events"),
		KafkaConsumerGroup:    getEnv("KAFKA_CONSUMER_GROUP", "analytics-workers"),
		KafkaProducerRetries:  getEnvInt("KAFKA_PRODUCER_RETRIES", 3),

		ShortCodeLength: getEnvInt("SHORT_CODE_LENGTH", 7),
		BaseURL:         getEnv("BASE_URL", "http://localhost:8081"),
	}

	return cfg, nil
}

// DSN returns the MySQL data source name.
func (c *Config) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
}

// ── helpers ──────────────────────────────────────────────────────────────────

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("[config] invalid int for %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("[config] invalid duration for %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}